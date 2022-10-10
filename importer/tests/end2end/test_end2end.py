import ast
import os
import time

import mock
from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse
from dynamic_models.models import FieldSchema, ModelSchema
from geonode.layers.models import Dataset
from geonode.resource.models import ExecutionRequest
from geonode.utils import OGC_Servers_Handler
from geoserver.catalog import Catalog
from importer import project_dir
from importer.tests.utils import ImporterBaseTestSupport

geourl = settings.GEODATABASE_URL


class BaseImporterEndToEndTest(ImporterBaseTestSupport):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.valid_gkpg = f"{project_dir}/tests/fixture/valid.gpkg"
        cls.valid_geojson = f"{project_dir}/tests/fixture/valid.geojson"
        cls.valid_kml = f"{project_dir}/tests/fixture/valid.kml"

        cls.url = reverse('importer_upload')
        ogc_server_settings = OGC_Servers_Handler(settings.OGC_SERVER)['default']

        _user, _password = ogc_server_settings.credentials

        cls.cat = Catalog(
            service_url=ogc_server_settings.rest,
            username=_user,
            password=_password
        )

    def setUp(self) -> None:
        self.admin = get_user_model().objects.get(username="admin")

    def tearDown(self) -> None:
        return super().tearDown()

    def _assertimport(self, payload, initial_name):
        self.client.force_login(self.admin)

        response = self.client.post(self.url, data=payload)
        self.assertEqual(201, response.status_code)

        # if is async, we must wait. It will wait for 1 min before raise exception
        if ast.literal_eval(os.getenv("ASYNC_SIGNALS", "False")):
            tentative = 1
            while ExecutionRequest.objects.get(exec_id=response.json().get("execution_id")) != ExecutionRequest.STATUS_FINISHED and tentative <= 6:
                time.sleep(10)
                tentative += 1
        if ExecutionRequest.objects.get(exec_id=response.json().get("execution_id")).status != ExecutionRequest.STATUS_FINISHED:
            raise Exception("Async still in progress after 1 min of waiting")

        # check if the dynamic model is created
        _schema_id = ModelSchema.objects.filter(name__icontains=initial_name)
        self.assertTrue(_schema_id.exists())
        schema_entity = _schema_id.first()
        self.assertTrue(FieldSchema.objects.filter(model_schema=schema_entity).exists())

        # Verify that ogr2ogr created the table with some data in it
        entries = ModelSchema.objects.filter(id=schema_entity.id).first()
        self.assertTrue(entries.as_model().objects.exists())

        # check if the resource is in geoserver
        resources = self.cat.get_resources()
        self.assertTrue(schema_entity.name in [y.name for y in resources])

        # check if the geonode resource exists
        dataset = Dataset.objects.filter(alternate=f"geonode:{schema_entity.name}")
        self.assertTrue(dataset.exists())


class ImporterGeoPackageImportTest(BaseImporterEndToEndTest):

    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data")
    def test_import_geopackage(self):
        payload = {
            "base_file": open(self.valid_gkpg, 'rb'),
        }
        initial_name = "stazioni_metropolitana"
        self._assertimport(payload, initial_name)


class ImporterGeoJsonImportTest(BaseImporterEndToEndTest):

    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data")
    def test_import_geojson(self):
        payload = {
            "base_file": open(self.valid_geojson, 'rb'),
        }
        initial_name = "valid"
        self._assertimport(payload, initial_name)


class ImporterKMLImportTest(BaseImporterEndToEndTest):

    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data")
    def test_import_shapefile(self):
        payload = {
            "base_file": open(self.valid_kml, 'rb'),
        }
        initial_name = "extruded_polygon"
        self._assertimport(payload, initial_name)
