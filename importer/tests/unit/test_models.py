from dynamic_models.models import ModelSchema, FieldSchema
from geonode.base.populate_test_data import create_single_dataset
from importer.models import ResourceHandlerInfo
from importer.tests.utils import TransactionImporterBaseTestSupport


class TestModelSchemaSignal(TransactionImporterBaseTestSupport):
    databases = ("default", "datastore")

    def setUp(self):
        self.resource = create_single_dataset(name="test_dataset")
        ResourceHandlerInfo.objects.create(
            resource=self.resource,
            handler_module_path="importer.handlers.shapefile.handler.ShapeFileHandler",
        )
        self.dynamic_model = ModelSchema.objects.create(
            name=self.resource.name, db_name="datastore"
        )
        self.dynamic_model_field = FieldSchema.objects.create(
            name="field",
            class_name="django.db.models.IntegerField",
            model_schema=self.dynamic_model,
        )

    def test_delete_dynamic_model(self):
        """
        Ensure that the dynamic model is deleted
        """
        # create needed resource handler info
        
        ResourceHandlerInfo.objects.create(
            resource=self.resource,
            handler_module_path="importer.handlers.gpkg.handler.GPKGFileHandler",
        )
        self.resource.delete()
        self.assertFalse(ModelSchema.objects.filter(name="test_dataset").exists())
        self.assertFalse(
            FieldSchema.objects.filter(
                model_schema=self.dynamic_model, name="field"
            ).exists()
        )
