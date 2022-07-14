import logging
from subprocess import PIPE, Popen

from celery import chord, group
from django.conf import settings
from django.utils import timezone
from dynamic_models.exceptions import InvalidFieldNameError, DynamicModelError
from dynamic_models.models import FieldSchema, ModelSchema
from geonode.resource.models import ExecutionRequest
from geonode.layers.models import Dataset
from importer.handlers.base import BaseHandler
from importer.handlers.gpkg.exceptions import InvalidGeopackageException
from importer.handlers.gpkg.tasks import SingleMessageErrorHandler
from importer.handlers.gpkg.utils import (GEOM_TYPE_MAPPING,
                                          STANDARD_TYPE_MAPPING, drop_dynamic_model_schema)
from importer.handlers.utils import should_be_imported
from geopackage_validator.validate import validate
from geonode.upload.utils import UploadLimitValidator
from osgeo import ogr
from geonode.services.serviceprocessors.base import \
    get_geoserver_cascading_workspace

logger = logging.getLogger(__name__)
from importer.celery_app import importer_app
from geonode.upload.api.exceptions import UploadParallelismLimitException
import hashlib


class GPKGFileHandler(BaseHandler):
    '''
    Handler to import GPK files into GeoNode data db
    It must provide the task_lists required to comple the upload
    '''
    TASKS_LIST = (
        "start_import",
        "importer.import_resource",
        "importer.publish_resource",
        "importer.create_gn_resource",
        # "importer.validate_upload", last task that will evaluate if there is any error coming from the execution. Maybe a chord?
    )

    @staticmethod
    def can_handle(_data) -> bool:
        '''
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        '''
        base = _data.get("base_file")
        if not base:
            return False
        return base.endswith('.gpkg') if isinstance(base, str) else base.name.endswith('.gpkg')

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps:
        Upload limit:
            - raise exception if the layer number of the gpkg is greater than the max upload per user
            - raise exception if the actual upload + the gpgk layer is greater than the max upload limit

        Gpkg definition:
            Codes table definition is here: https://github.com/PDOK/geopackage-validator#what-does-it-do
            RQ1: Layer names must start with a letter, and valid characters are lowercase a-z, numbers or underscores.
            RQ2: Layers must have at least one feature.
            RQ13: It is required to give all GEOMETRY features the same default spatial reference system
            RQ14: The geometry_type_name from the gpkg_geometry_columns table must be one of POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, or MULTIPOLYGON
            RQ15: All table geometries must match the geometry_type_name from the gpkg_geometry_columns table
            RC2: It is recommended to give all GEOMETRY type columns the same name.
        """
        # getting the upload limit validation
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()
        actual_upload = upload_validator._get_parallel_uploads_count()
        max_upload = upload_validator._get_max_parallel_uploads()

        layers = ogr.Open(files.get("base_file"))
        # for the moment we skip the dyanamic model creation
        layers_count = len(layers)

        if layers_count >= max_upload:
            raise UploadParallelismLimitException(
                detail=f"The number of layers in the gpkg {layers_count} is greater than " \
                f"the max parallel upload permitted: {max_upload} " \
                f"please upload a smaller file"
            )
        elif layers_count + actual_upload >= max_upload:
            raise UploadParallelismLimitException(
                detail=f"With the provided gpkg, the number of max parallel upload will exceed the limit of {max_upload}"
            )

        validator = validate(
            gpkg_path=files.get("base_file"),
            validations='RQ1, RQ2, RQ13, RQ14, RQ15, RC2'
        )
        if not validator[-1]:
            raise InvalidGeopackageException(validator[0])

        return True

    @staticmethod
    def extract_params_from_data(_data):
        '''
        Remove from the _data the params that needs to save into the executionRequest object
        all the other are returned
        '''
        return {
            "skip_existing_layers": _data.pop('skip_existing_layers', "False"),
            "override_existing_layer": _data.pop('override_existing_layer', "False"),
            "store_spatial_file": _data.pop("store_spatial_files", "True"),
        }, _data

    @staticmethod
    def extract_resource_to_publish(files, layer_name, alternate):
        layers = ogr.Open(files.get("base_file"))
        if not layers:
            return []
        return [
            {
                "name": alternate or layer_name,
                "crs" : (
                    f"{_l.GetSpatialRef().GetAuthorityName(None)}:{_l.GetSpatialRef().GetAuthorityCode('PROJCS')}"
                    if _l.GetSpatialRef() else None
                )
            } 
            for _l in layers
            if _l.GetName() == layer_name
        ]

    @staticmethod
    def create_error_log(exc, task_name, *args):
        '''
        Method needed to personalize the log based on the resource type
        args[-1] should contain the layer alternate
        '''
        return f"Task: {task_name} raised an error during actions for layer: {args[-1]}: {exc}"

    @staticmethod
    def publish_resources(resources, catalog, store, workspace):
        '''
        Given a list of strings (which rappresent the table on geoserver)
        Will publish the resorces on geoserver
        '''
        for _resource in resources:
            try:
                catalog.publish_featuretype(
                    name=_resource.get("name"),
                    store=store,
                    native_crs=_resource.get("crs"),
                    srs=_resource.get("crs"),
                    jdbc_virtual_table=_resource.get("name")
                )
            except Exception as e:
                if f"Resource named {_resource.get('name')} already exists in store:" in str(e):
                    continue
                raise e
        return True

    def import_resource(self, files: dict, execution_id: str, **kwargs) -> str:
        '''
        Main function to import the resource.
        Internally will call the steps required to import the 
        data inside the geonode_data database
        '''
        layers = ogr.Open(files.get("base_file"))
        # for the moment we skip the dyanamic model creation
        layer_count = len(layers)
        logger.info(f"Total number of layers available: {layer_count}")
        _exec = self._get_execution_request_object(execution_id)
        dynamic_model = None
        try:
            # start looping on the layers available
            for index, layer in enumerate(layers, start=1):

                layer_name = layer.GetName().lower()

                should_be_overrided = _exec.input_params.get("override_existing_layer")
                # should_be_imported check if the user+layername already exists or not
                if should_be_imported(
                    layer_name, _exec.user,
                    skip_existing_layer=_exec.input_params.get("skip_existing_layer"),
                    override_existing_layer=should_be_overrided
                ) and layer.GetGeometryColumn() is not None:
                    #update the execution request object
                    self._update_execution_request(
                        execution_id=execution_id,
                        last_updated=timezone.now(),
                        log=f"setting up dynamic model for layer: {layer_name} complited: {(100*index)/layer_count}%"
                    )
                    # setup dynamic model and retrieve the group task needed for tun the async workflow
                    dynamic_model, alternate, celery_group = self.setup_dynamic_model(layer, execution_id, should_be_overrided, username=_exec.user)
                    # evaluate if a new alternate is created by the previous flow
                    # create the async task for create the resource into geonode_data with ogr2ogr
                    ogr_res = gpkg_ogr2ogr.s(execution_id, files, layer.GetName().lower(), should_be_overrided, alternate)
                    # prepare the async chord workflow with the on_success and on_fail methods
                    workflow = chord(
                        group(celery_group.set(link_error=['gpkg_error_callback']), ogr_res.set(link_error=['gpkg_error_callback']))
                    )(gpkg_next_step.s(
                        execution_id,
                        str(self), # passing the handler module path
                        "importer.import_resource",
                        layer_name,
                        alternate
                    ))
        except Exception as e:
            logger.error(e)
            if dynamic_model:
                '''
                In case of fail, we want to delete the dynamic_model schema and his field
                to keep the DB in a consistent state
                '''
                drop_dynamic_model_schema(dynamic_model)
            raise e
        return

    def setup_dynamic_model(self, layer: ogr.Layer, execution_id: str, should_be_overrided: bool, username: str):
        '''
        Extract from the geopackage the layers name and their schema
        after the extraction define the dynamic model instances
        Returns:
            - dynamic_model as model, so the actual dynamic instance
            - alternate -> the alternate of the resource which contains (if needed) the uuid
            - celery_group -> the celery group of the field creation
        '''

        layer_name = layer.GetName().lower()
        workspace = get_geoserver_cascading_workspace(create=False)
        user_datasets = Dataset.objects.filter(owner=username, alternate=f'{workspace.name}:{layer_name}')
        dynamic_schema = ModelSchema.objects.filter(name=layer_name)

        dynamic_schema_exists = dynamic_schema.exists()
        dataset_exists = user_datasets.exists()

        if dataset_exists and dynamic_schema_exists and should_be_overrided:
            '''
            If the user have a dataset, the dynamic model has already been created and is in override mode,
            we just take the dynamic_model to override the existing one
            '''
            dynamic_schema = dynamic_schema.get()
        elif not dataset_exists and not dynamic_schema_exists and should_be_overrided:
            '''
            If the user doesnt have any dataset or foi schema associated and the user is tring to override
            we raise an error
            '''
            logger.error("The user is trying to override a dataset that doesnt belongs to it. Please fix the geopackage and try")
            raise InvalidGeopackageException(detail="The user is trying to override a dataset that doesnt belongs to it. Please fix the geopackage and try")
        elif (
                dataset_exists and not dynamic_schema_exists
            ) or (
                not dataset_exists and not dynamic_schema_exists
            ):
            '''
            cames here when is a new brand upload or when (for any reasons) the dataset exists but the
            dynamic model has not been created before
            '''
            dynamic_schema = ModelSchema.objects.create(
                name=layer_name,
                db_name="datastore",
                managed=False,
                db_table_name=layer_name
            )
        elif (
            not dataset_exists and dynamic_schema_exists
        ) or (
            dataset_exists and dynamic_schema_exists and not should_be_overrided
        ):
            '''
            it comes here when the layer should not be overrided so we append the UUID
            to the layer to let it proceed to the next steps
            '''
            layer_name = self._create_alternate(layer_name, execution_id)
            dynamic_schema, _ = ModelSchema.objects.get_or_create(
                name=layer_name,
                db_name="datastore",
                managed=False,
                db_table_name=layer_name
            )
        else:
            raise InvalidGeopackageException("Error during the upload of the gpkg file. The dataset does not exists")

        # define standard field mapping from ogr to django
        dynamic_model, celery_group = self.create_dynamic_model_fields(
            layer=layer,
            dynamic_model_schema=dynamic_schema,
            overwrite=should_be_overrided,
            execution_id=execution_id,
            layer_name=layer_name
        )
        return dynamic_model, layer_name, celery_group

    def create_dynamic_model_fields(self, layer: str, dynamic_model_schema: ModelSchema, overwrite: bool, execution_id: str, layer_name: str):
        # retrieving the field schema from ogr2ogr and converting the type to Django Types
        layer_schema = [
            {"name": x.name.lower(), "class_name": self._get_type(x), "null": True}
            for x in layer.schema
        ]
        if layer.GetGeometryColumn():
            # the geometry colum is not returned rom the layer.schema, so we need to extract it manually
            layer_schema += [
                {
                    "name": layer.GetGeometryColumn(),
                    "class_name": GEOM_TYPE_MAPPING.get(ogr.GeometryTypeToName(layer.GetGeomType()))
                }
            ]

        # ones we have the schema, here we create a list of chunked value
        # so the async task will handle max of 30 field per task
        list_chunked = [layer_schema[i:i + 30] for i in range(0, len(layer_schema), 30)]

        # definition of the celery group needed to run the async workflow.
        # in this way each task of the group will handle only 30 field
        celery_group = group(gpkg_handler.s(execution_id, schema, dynamic_model_schema.id, overwrite, layer_name) for schema in list_chunked)

        return dynamic_model_schema, celery_group

    def _update_execution_request(self, execution_id: str, **kwargs):
        ExecutionRequest.objects.filter(exec_id=execution_id).update(
            status=ExecutionRequest.STATUS_RUNNING, **kwargs
        )

    def _create_alternate(self, layer_name, execution_id):
        _hash = hashlib.md5(
            f"{layer_name}_{execution_id}".encode('utf-8')
        ).hexdigest()
        alternate = f"{layer_name}_{_hash}"
        if len(alternate) >= 64: # 64 is the max table lengh in postgres
            return f"{layer_name[:50]}{_hash[:14]}"
        return alternate

    def _get_execution_request_object(self, execution_id: str):
        return ExecutionRequest.objects.filter(exec_id=execution_id).first()

    def _get_type(self, _type: str):
        '''
        Used to get the standard field type in the dynamic_model_field definition
        '''
        return STANDARD_TYPE_MAPPING.get(ogr.FieldDefn.GetTypeName(_type))

@importer_app.task(
    base=SingleMessageErrorHandler,
    name="importer.gpkg_handler",
    queue="importer.gpkg_handler",
    max_retries=1,
    acks_late=False,
    ignore_result=False,
    task_track_started=True
)
def gpkg_handler(execution_id: str, fields: dict, dynamic_model_schema_id: int, overwrite: bool, layer_name: str):
    def _create_field(dynamic_model_schema, field, _kwargs):
        # common method to define the Field Schema object
        return FieldSchema(
                    name=field['name'],
                    class_name=field['class_name'],
                    model_schema=dynamic_model_schema,
                    kwargs=_kwargs
                )
    '''
    Create the single dynamic model field for each layer. Is made by a batch of 30 field
    '''
    dynamic_model_schema = ModelSchema.objects.filter(id=dynamic_model_schema_id)
    if not dynamic_model_schema.exists():
        raise DynamicModelError(f"The model with id {dynamic_model_schema_id} does not exists. It may be deleted from the error callback tasks")

    dynamic_model_schema = dynamic_model_schema.first()

    row_to_insert = []
    for field in fields:
        # setup kwargs for the class provided
        if field['class_name'] is None or field['name'] is None:
            logger.error(f"Error during the field creation. The field or class_name is None {field}")
            raise InvalidFieldNameError(f"Error during the field creation. The field or class_name is None {field}")

        _kwargs = {"null": field.get('null', True)}
        if field['class_name'].endswith('CharField'):
            _kwargs = {**_kwargs, **{"max_length": 255}}
    
        # if is a new creation we generate the field model from scratch
        if not overwrite:
            row_to_insert.append(_create_field(dynamic_model_schema, field, _kwargs))
        else:
            # otherwise if is an overwrite, we update the existing one and create the one that does not exists
            _field_exists = FieldSchema.objects.filter(name=field['name'], model_schema=dynamic_model_schema)
            if _field_exists.exists():
                _field_exists.update(
                    class_name=field['class_name'],
                    model_schema=dynamic_model_schema,
                    kwargs=_kwargs
                )
            else:    
                row_to_insert.append(_create_field(dynamic_model_schema, field, _kwargs))
    
    if row_to_insert:
        # the build creation improves the overall permformance with the DB
        FieldSchema.objects.bulk_create(row_to_insert, 30)

    del row_to_insert
    return "dynamic_model", layer_name, execution_id


@importer_app.task(
    base=SingleMessageErrorHandler,
    name="importer.gpkg_ogr2ogr",
    queue="importer.gpkg_ogr2ogr",
    max_retries=1,
    acks_late=False,
    ignore_result=False,
    task_track_started=True
)
def gpkg_ogr2ogr(execution_id: str, files: dict, original_name:str, override_layer=False, alternate=None):
    '''
    Perform the ogr2ogr command to import he gpkg inside geonode_data
    If the layer should be overwritten, the option is appended dynamically
    '''

    ogr_exe = "/usr/bin/ogr2ogr"
    _uri = settings.GEODATABASE_URL.replace("postgis://", "")
    db_user, db_password = _uri.split('@')[0].split(":")
    db_host, db_port = _uri.split('@')[1].split('/')[0].split(":")
    db_name = _uri.split('@')[1].split("/")[1]

    options = '--config PG_USE_COPY YES '
    options += '-f PostgreSQL PG:" dbname=\'%s\' host=%s port=%s user=\'%s\' password=\'%s\' " ' \
                % (db_name, db_host, db_port, db_user, db_password)
    options += files.get("base_file") + " "
    options += '-lco DIM=2 '
    options += f"-nln {alternate} {original_name}"

    if override_layer:
        options += " -overwrite"

    commands = [ogr_exe] + options.split(" ")
    
    process = Popen(' '.join(commands), stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = process.communicate()
    if stderr is not None and stderr != b'' and b'Warning' not in stderr:
        raise Exception(stderr)
    return "ogr2ogr", alternate, execution_id


@importer_app.task(
    name="importer.gpkg_next_step",
    queue="importer.gpkg_next_step",
    task_track_started=True
)
def gpkg_next_step(_, execution_id: str, handlers_module_path, actual_step: str, layer_name: str, alternate:str):
    '''
    If the ingestion of the resource is successfuly, the next step for the layer is called
    '''
    from importer.celery_tasks import import_orchestrator, orchestrator

    _exec = orchestrator.get_execution_object(execution_id)

    _files = _exec.input_params.get("files")
    # at the end recall the import_orchestrator for the next step
    import_orchestrator.apply_async(
        (_files, execution_id, handlers_module_path, actual_step, layer_name, alternate)
    )
    return "gpkg_next_step", alternate, execution_id

@importer_app.task(name='gpkg_error_callback')
def error_callback(*args, **kwargs):
    # revert eventually the import in ogr2ogr or the creation of the model in case of failing
    alternate = args[0].args[-1]
    schema_model = ModelSchema.objects.filter(name=alternate).first()

    drop_dynamic_model_schema(schema_model)

    return 'error'
