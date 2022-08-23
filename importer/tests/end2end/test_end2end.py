import os
from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse
from dynamic_models.models import FieldSchema, ModelSchema
from geonode.layers.models import Dataset
import mock
from importer import project_dir
from importer.tests.utils import ImporterBaseTestSupport

geourl = settings.GEODATABASE_URL

class ImporterEndToEndTest(ImporterBaseTestSupport):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.valid_gkpg = f"{project_dir}/tests/fixture/valid.gpkg"
        cls.invalid_gkpg = f"{project_dir}/tests/fixture/invalid.gpkg"
        cls.invalid_geojson = f"{project_dir}/tests/fixture/valid.geojson"
        cls.url = reverse('importer_upload')

    def setUp(self) -> None:
        self.admin = get_user_model().objects.get(username="admin")

    def tearDown(self) -> None:
        return super().tearDown()



    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data")
    def test_import_geopackage(self):
        try:
            self.client.force_login(self.admin)
            payload = {
                "base_file": open(self.valid_gkpg, 'rb'),
            }        
            response = self.client.post(self.url, data=payload)
            self.assertEqual(201, response.status_code)

            # check if the dynamic model is created
            _schema_id = ModelSchema.objects.filter(name__icontains='stazioni_metropolitana')
            self.assertTrue(_schema_id.exists())
            schema_entity = _schema_id.first()
            self.assertTrue(FieldSchema.objects.filter(model_schema=schema_entity).exists())

            # Verify that ogr2ogr created the table with some data in it
            entries = ModelSchema.objects.filter(id=schema_entity.id).first()
            self.assertTrue(entries.as_model().objects.exists())

            # check if the resource is in geoserver

            # check if the geonode resource exists
            self.assertTrue()
        finally:
            res = Dataset.objects.filter(alternate='geonode:stazioni_metropolitana')
            if res.exists():
                res = res.first()
                res.delete()
