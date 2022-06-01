from django.conf import settings
from importer.handlers.base import GEOM_TYPE_MAPPING, STANDARD_TYPE_MAPPING, AbstractHandler
from dynamic_models.models import ModelSchema, FieldSchema
import os
import shapely.ops
from subprocess import Popen, PIPE
from osgeo import ogr


class GPKGFileHandler(AbstractHandler):
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

    def import_resource(self, files):
        layers = ogr.Open(files.get("base_file"))
        # for the moment we skip the dyanamic model creation
        #for layer in layers:
        #    self._setup_dynamic_model(layer)
        stdout = self._run_ogr2ogr_import(files)
        return stdout

    def _setup_dynamic_model(self, layer):
        # TODO: finish the creation, is raising issues due the NONE value of the table
        model_schema, _ = ModelSchema.objects.get_or_create(name=layer.GetName(), db_name="datastore")
        # define standard field mapping from ogr to django
        dynamic_model = self.create_model_instance(layer, model_schema)
        return dynamic_model.as_model()

    def create_model_instance(self, layer, model_schema):
        layer_schema = [{"name": x.name.lower(), "class_name": self._get_type(x)} for x in layer.schema]
        # define the geometry type
        layer_schema += [
            {
                "name": layer.GetGeometryColumn(),
                "class_name": GEOM_TYPE_MAPPING.get(ogr.GeometryTypeToName(layer.GetGeomType()))
            }
        ]

        for field in layer_schema:
            FieldSchema.objects.create(
                name=field['name'],
                class_name=field['class_name'],
                model_schema=model_schema,
                #kwargs=_kwargs
            )
        return model_schema.as_model()


    def _run_ogr2ogr_import(self, files):
        ogr_exe = "/usr/bin/ogr2ogr"
        _uri = settings.GEODATABASE_URL.replace("postgis://", "")
        db_user, db_password = _uri.split('@')[0].split(":")
        db_host, db_port = _uri.split('@')[1].split('/')[0].split(":")
        db_name = _uri.split('@')[1].split("/")[1]
        
        options = '-progress '
        options += '--config PG_USE_COPY YES '
        options += '-f PostgreSQL PG:" dbname=\'%s\' host=%s port=%s user=\'%s\' password=\'%s\' " ' \
                   % (db_name, db_host, db_port, db_user, db_password)
        options += files.get("base_file") + " "
        options += '-lco DIM=2 '
        options += '-overwrite '

        commands = [ogr_exe] + options.split(" ")
        
        process = Popen(' '.join(commands), stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = process.communicate()
        if stderr is not None and stderr != b'':
            raise Exception(stderr)
        return stdout


    def _get_type(self, _type):
        return STANDARD_TYPE_MAPPING.get(ogr.FieldDefn.GetTypeName(_type))