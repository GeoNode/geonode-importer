from importer.handlers.base import GEOM_TYPE_MAPPING, STANDARD_TYPE_MAPPING, AbstractHandler
from dynamic_models.models import ModelSchema, FieldSchema
import os
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

    def start_import(self, files):
        layers = ogr.Open(files.get("base_file"))
        #for layer in layers:
        #    self._setup_dynamic_model(layer)
        self._run_ogr2ogr_import()
        pass

    def _setup_dynamic_model(self, layer):
        # TODO: finish the creation, is raising issues due the NONE value of the table
        model_schema, _ = ModelSchema.objects.get_or_create(name=layer.GetName())
        _kwargs = {"max_length": 255, "null": True}
        # define standard field mapping from ogr to django
        layer_schema = [{"name": x.name.lower(), "class_name": self._get_type(x)} for x in layer.schema]
        # define the geometry type
        layer_schema += [
            {
                "name": layer.GetGeometryColumn(),
                "class_name": GEOM_TYPE_MAPPING.get(ogr.GeometryTypeToName(layer.GetGeomType()))
            }
        ]
        model_schema.refresh_from_db()
        for field in layer_schema:
            FieldSchema.objects.create(
                name=field['name'],
                class_name=field['class_name'],
                model_schema=model_schema,
                #kwargs=_kwargs
            )
            model_schema.refresh_from_db()
        return model_schema.as_model()

    def _get_type(self, _type):
        return STANDARD_TYPE_MAPPING.get(ogr.FieldDefn.GetTypeName(_type))