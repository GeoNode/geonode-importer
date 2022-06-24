import logging
from subprocess import PIPE, Popen

from celery import chord, group
from django.conf import settings
from django.utils import timezone
from dynamic_models.exceptions import InvalidFieldNameError, DynamicModelError
from dynamic_models.models import FieldSchema, ModelSchema
from geonode.resource.models import ExecutionRequest
from importer.celery_tasks import ErrorBaseTaskClass
from importer.handlers.base import AbstractHandler
from importer.handlers.gpkg.exceptions import InvalidGeopackageException
from importer.handlers.gpkg.tasks import SingleMessageErrorHandler
from importer.handlers.gpkg.utils import (GEOM_TYPE_MAPPING,
                                          STANDARD_TYPE_MAPPING)
from importer.handlers.utils import should_be_imported
from geopackage_validator.validate import validate
from geonode.upload.utils import UploadLimitValidator
from osgeo import ogr

logger = logging.getLogger(__name__)
from importer.celery_app import importer_app
from geonode.upload.api.exceptions import UploadParallelismLimitException


class GPKGFileHandler(AbstractHandler):
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

    def is_valid(self, files, user):
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

    def create_error_log(self, task_name, *args):
        '''
        Method needed to personalize the log based on the resource type
        '''
        return f"Task: {task_name} raised an error during actions for layer: {args[-1]}"

    def import_resource(self, files: dict, execution_id: str, **kwargs) -> str:
        '''
        Main function to import the resource.
        Internally will cal the steps required to import the 
        data inside the geonode_data database
        '''
        layers = ogr.Open(files.get("base_file"))
        # for the moment we skip the dyanamic model creation
        layer_count = len(layers)
        logger.info(f"Total number of layers available: {layer_count}")
        _exec = self._get_execution_request_object(execution_id)
        
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
                _, use_uuid, layer_res = self._setup_dynamic_model(layer, execution_id, should_be_overrided)
                # evaluate if a new alternate is created by the previous flow
                alternate = layer_name if not use_uuid else f"{layer_name}_{execution_id.replace('-', '_')}"
                # create the async task for create the resource into geonode_data with ogr2ogr
                ogr_res = gpkg_ogr2ogr.s(execution_id, files, layer.GetName().lower(), should_be_overrided, alternate)

                # prepare the async chord workflow with the on_success and on_fail methods
                workflow = chord(
                    [layer_res.set(link_error=['gpkg_error_callback']), ogr_res.set(link_error=['gpkg_error_callback'])],
                    body=execution_id
                )(gpkg_next_step.s(execution_id, "importer.import_resource", layer_name, alternate))

        return

    def _setup_dynamic_model(self, layer, execution_id: str, should_be_overrided: bool):
        '''
        Extract from the geopackage the layers name and their schema
        after the extraction define the dynamic model instances
        '''
        use_uuid = False

        layer_name = layer.GetName().lower()
        foi_schema, created = ModelSchema.objects.get_or_create(
            name=layer.GetName().lower(),
            db_name="datastore",
            managed=False,
            db_table_name=layer_name
        )
        if not created and not should_be_overrided:
            # if the model schema already exists, means that a layer with that name already exists
            # so we are going to append the executionID to the layer name to have a 
            # unique alternate for the layer
            use_uuid = True
            layer_name = f"{layer.GetName().lower()}_{execution_id.replace('-', '_')}"
            foi_schema, created = ModelSchema.objects.get_or_create(
                name=f"{layer.GetName().lower()}_{execution_id.replace('-', '_')}",
                db_name="datastore",
                managed=False,
                db_table_name=layer_name
            )
        # define standard field mapping from ogr to django
        dynamic_model, res = self.create_dynamic_model_fields(
            layer=layer,
            dynamic_model_schema=foi_schema,
            overwrite=should_be_overrided,
            execution_id=execution_id,
            layer_name=layer_name
        )
        return dynamic_model, use_uuid, res

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
        job = group(gpkg_handler.s(execution_id, schema, dynamic_model_schema.id, overwrite, layer_name) for schema in list_chunked)

        return dynamic_model_schema.as_model(), job

    def _update_execution_request(self, execution_id: str, **kwargs):
        ExecutionRequest.objects.filter(exec_id=execution_id).update(
            status=ExecutionRequest.STATUS_RUNNING, **kwargs
        )

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
    ignore_result=False
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


@importer_app.task(
    base=SingleMessageErrorHandler,
    name="importer.gpkg_ogr2ogr",
    queue="importer.gpkg_ogr2ogr",
    max_retries=1,
    acks_late=False,
    ignore_result=False
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
    if stderr is not None and stderr != b'':
        raise Exception(stderr)
    return stdout.decode()


@importer_app.task(
    base=ErrorBaseTaskClass,
    name="importer.gpkg_next_step",
    queue="importer.gpkg_next_step"
)
def gpkg_next_step(_, execution_id: str, actual_step: str, layer_name: str, alternate:str):
    '''
    If the ingestion of the resource is successfuly, the next step for the layer is called
    '''
    from importer.views import import_orchestrator, orchestrator

    _exec = orchestrator.get_execution_object(execution_id)

    _files = _exec.input_params.get("files")
    _store_spatial_files = _exec.input_params.get("store_spatial_files")
    _user = _exec.user
    # at the end recall the import_orchestrator for the next step
    import_orchestrator.apply_async(
        (_files, _store_spatial_files, _user.username, execution_id, actual_step, layer_name, alternate)
    )

@importer_app.task(name='gpkg_error_callback')
def error_callback(*args, **kwargs):
    from dynamic_models.schema import ModelSchemaEditor
    # revert eventually the import in ogr2ogr or the creation of the model in case of failing
    alternate = args[0].args[-1]
    
    schema_model = ModelSchema.objects.filter(name=alternate)
    if schema_model.exists():
        schema = ModelSchemaEditor(
            initial_model=alternate,
            db_name="datastore"
        )
        try:
            schema.drop_table(schema_model.first().as_model())
        except Exception as e:
            logger.warning(e.args[0])

        schema_model.delete()

    return 'error'