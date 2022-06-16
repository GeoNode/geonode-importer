import logging
import os
from subprocess import PIPE, Popen

from django.conf import settings
from django.utils import timezone
from dynamic_models.models import FieldSchema, ModelSchema
from geonode.resource.models import ExecutionRequest
from importer.handlers.base import (GEOM_TYPE_MAPPING, STANDARD_TYPE_MAPPING,
                                    AbstractHandler)
from osgeo import ogr
from celery import chord, group

from importer.handlers.utils import should_be_imported

logger = logging.getLogger(__name__)
from importer.celery_app import importer_app


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
    )

    def is_valid(self, files):
        """
        Define basic validation steps
        """        
        return all([os.path.exists(x) for x in files.values()])

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
        for index, layer in enumerate(layers, start=1):
            # should_be_imported check if the user+layername already exists or not
            layer_name = layer.GetName()
            if should_be_imported(layer_name, _exec.user):
                self._update_execution_request(
                    execution_id=execution_id,
                    last_updated=timezone.now(),
                    log=f"setting up dynamic model for layer: {layer_name} complited: {(100*index)/layer_count}%"
                )
                # setup dynamic model and retrieve the group job needed for tun the async workflow
                _, use_uuid, layer_res = self._setup_dynamic_model(layer, execution_id)

                alternate = layer_name if not use_uuid else f"{layer_name}_{execution_id}"
                ogr_res = gpkg_ogr2ogr.s(files, layer.GetName(), alternate)
                workflow = chord(
                    [layer_res, ogr_res],
                    body=execution_id
                )(gpkg_next_step.s(execution_id, "importer.import_resource", layer_name, alternate).set(link_error=['gpkg_failure_step']))

        return

    def _setup_dynamic_model(self, layer, execution_id):
        '''
        Extract from the geopackage the layers name and their schema
        after the extraction define the dynamic model instances
        '''
        use_uuid = False
        # TODO: finish the creation, is raising issues due the NONE value of the table
        foi_schema, created = ModelSchema.objects.get_or_create(
            name=layer.GetName(),
            db_name="datastore",
            is_managed=False,
            use_applable_as_table_prefix=False
        )
        if not created:
            use_uuid = True
            foi_schema, created = ModelSchema.objects.get_or_create(
                name=f"{layer.GetName()}_{execution_id}",
                db_name="datastore",
                is_managed=False,
                use_applable_as_table_prefix=False
            )
        # define standard field mapping from ogr to django
        dynamic_model, res = self.create_dynamic_model_fields(layer=layer, dynamic_model_schema=foi_schema)
        return dynamic_model, use_uuid, res

    def create_dynamic_model_fields(self, layer, dynamic_model_schema):
        layer_schema = [
            {"name": x.name.lower(), "class_name": self._get_type(x), "null": True}
            for x in layer.schema
        ]
        layer_schema += [
            {
                "name": layer.GetGeometryColumn(),
                "class_name": GEOM_TYPE_MAPPING.get(ogr.GeometryTypeToName(layer.GetGeomType()))
            }
        ]

        list_chunked = [layer_schema[i:i + 50] for i in range(0, len(layer_schema), 50)]
        job = group(gpkg_handler.s(schema, dynamic_model_schema.id) for schema in list_chunked)
        return dynamic_model_schema.as_model(), job

    def _update_execution_request(self, execution_id, **kwargs):
        ExecutionRequest.objects.filter(exec_id=execution_id).update(
            status=ExecutionRequest.STATUS_RUNNING, **kwargs
        )

    def _get_execution_request_object(self, execution_id):
        return ExecutionRequest.objects.filter(exec_id=execution_id).first()

    def _get_type(self, _type):
        '''
        Used to get the standard field type in the dynamic_model_field definition
        '''
        return STANDARD_TYPE_MAPPING.get(ogr.FieldDefn.GetTypeName(_type))


@importer_app.task(
    bind=True,
    name="importer.gpkg_handler",
    queue="importer.gpkg_handler",
    max_retries=1,
    acks_late=False,
    ignore_result=False
)
def gpkg_handler(self, fields, dynamic_model_schema_id):
    dynamic_model_schema = ModelSchema.objects.get(id=dynamic_model_schema_id)
    for field in fields:
        if field['class_name'] is None:
            logger.error(f"Field named {field['name']} cannot be importer, the field is not recognized")
            return
        _kwargs = {"null": field.get('null', True)}
        if field['class_name'].endswith('CharField'):
            _kwargs = {**_kwargs, **{"max_length": 255}}
        
        FieldSchema.objects.create(
            name=field['name'],
            class_name=field['class_name'],
            model_schema=dynamic_model_schema,
            kwargs=_kwargs
        )

@importer_app.task(
    bind=True,
    name="importer.gpkg_ogr2ogr",
    queue="importer.gpkg_ogr2ogr",
    max_retries=1,
    acks_late=False,
    ignore_result=False
)
def gpkg_ogr2ogr(self, files, original_name, alternate):
    '''
    Perform the ogr2ogr command to import he gpkg inside geonode_data
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

    commands = [ogr_exe] + options.split(" ")
    
    process = Popen(' '.join(commands), stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = process.communicate()
    if stderr is not None and stderr != b'':
        raise Exception(stderr)
    return stdout.decode()


@importer_app.task(
    bind=True,
    name="importer.gpkg_next_step",
    queue="importer.gpkg_next_step"
)
def gpkg_next_step(self, _, execution_id, actual_step, layer_name, alternate):
    from importer.views import import_orchestrator, importer

    _exec = importer.get_execution_object(execution_id)

    _files = _exec.input_params.get("files")
    _store_spatial_files = _exec.input_params.get("store_spatial_files")
    _user = _exec.user
    # at the end recall the import_orchestrator for the next step
    import_orchestrator.apply_async(
        (_files, _store_spatial_files, _user.username, execution_id, actual_step)
    )


@importer_app.task(
    bind=True,
    name="importer.gpkg_failure_step",
    queue="importer.gpkg_failure_step"
)
def gpkg_failure_step(self, execution_id):

    print("helloworld")
    print(execution_id)
