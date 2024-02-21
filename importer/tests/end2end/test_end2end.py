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
import gisdata
from geonode.base.populate_test_data import create_single_dataset

geourl = settings.GEODATABASE_URL


class BaseImporterEndToEndTest(ImporterBaseTestSupport):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.valid_gkpg = f"{project_dir}/tests/fixture/valid.gpkg"
        cls.valid_geojson = f"{project_dir}/tests/fixture/valid.geojson"
        file_path = gisdata.VECTOR_DATA
        filename = os.path.join(file_path, "san_andres_y_providencia_highway.shp")
        cls.valid_shp = {
            "base_file": filename,
            "dbf_file": f"{file_path}/san_andres_y_providencia_highway.dbf",
            "prj_file": f"{file_path}/san_andres_y_providencia_highway.prj",
            "shx_file": f"{file_path}/san_andres_y_providencia_highway.shx",
        }
        cls.valid_kml = f"{project_dir}/tests/fixture/valid.kml"

        cls.url = reverse("importer_upload")
        ogc_server_settings = OGC_Servers_Handler(settings.OGC_SERVER)["default"]

        _user, _password = ogc_server_settings.credentials

        cls.cat = Catalog(
            service_url=ogc_server_settings.rest, username=_user, password=_password
        )

    def setUp(self) -> None:
        self.admin = get_user_model().objects.get(username="admin")
        for el in Dataset.objects.all():
            el.delete()

    def tearDown(self) -> None:
        super().tearDown()
        for el in Dataset.objects.all():
            el.delete()

    def _assertimport(self, payload, initial_name, overwrite=False, last_update=None):
        try:
            self.client.force_login(self.admin)

            response = self.client.post(self.url, data=payload)
            self.assertEqual(201, response.status_code)

            # if is async, we must wait. It will wait for 1 min before raise exception
            if ast.literal_eval(os.getenv("ASYNC_SIGNALS", "False")):
                tentative = 1
                while (
                    ExecutionRequest.objects.get(
                        exec_id=response.json().get("execution_id")
                    )
                    != ExecutionRequest.STATUS_FINISHED
                    and tentative <= 10
                ):
                    time.sleep(10)
                    tentative += 1
            if (
                ExecutionRequest.objects.get(
                    exec_id=response.json().get("execution_id")
                ).status
                != ExecutionRequest.STATUS_FINISHED
            ):
                raise Exception("Async still in progress after 1 min of waiting")

            # check if the dynamic model is created
            if os.getenv("IMPORTER_ENABLE_DYN_MODELS", False):
                _schema_id = ModelSchema.objects.filter(name__icontains=initial_name)
                self.assertTrue(_schema_id.exists())
                schema_entity = _schema_id.first()
                self.assertTrue(
                    FieldSchema.objects.filter(model_schema=schema_entity).exists()
                )

                # Verify that ogr2ogr created the table with some data in it
                entries = ModelSchema.objects.filter(id=schema_entity.id).first()
                self.assertTrue(entries.as_model().objects.exists())

            # check if the geonode resource exists
            dataset = Dataset.objects.filter(
                alternate__icontains=f"geonode:{initial_name}"
            )
            self.assertTrue(dataset.exists())

            # check if the resource is in geoserver
            resources = self.cat.get_resources()
            self.assertTrue(dataset.first().title in [y.name for y in resources])
            if overwrite:
                self.assertTrue(dataset.first().last_updated > last_update)
        finally:
            dataset = Dataset.objects.filter(
                alternate__icontains=f"geonode:{initial_name}"
            )
            dataset.delete()


class ImporterGeoPackageImportTest(BaseImporterEndToEndTest):
    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(
        GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data"
    )
    def test_import_geopackage(self):
        layer = self.cat.get_layer("geonode:stazioni_metropolitana")
        if layer:
            self.cat.delete(layer)
        payload = {
            "base_file": open(self.valid_gkpg, "rb"),
        }
        initial_name = "stazioni_metropolitana"
        self._assertimport(payload, initial_name)
        layer = self.cat.get_layer("geonode:stazioni_metropolitana")
        if layer:
            self.cat.delete(layer)

    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(
        GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data"
    )
    def test_import_gpkg_overwrite(self):
        prev_dataset = create_single_dataset(name="stazioni_metropolitana")

        layer = self.cat.get_layer("geonode:stazioni_metropolitana")
        if layer:
            self.cat.delete(layer)
        payload = {
            "base_file": open(self.valid_gkpg, "rb"),
        }
        initial_name = "stazioni_metropolitana"
        payload["overwrite_existing_layer"] = True
        self._assertimport(
            payload, initial_name, overwrite=True, last_update=prev_dataset.last_updated
        )
        layer = self.cat.get_layer("geonode:stazioni_metropolitana")
        if layer:
            self.cat.delete(layer)


class ImporterGeoJsonImportTest(BaseImporterEndToEndTest):
    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(
        GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data"
    )
    def test_import_geojson(self):
        layer = self.cat.get_layer("geonode:valid")
        if layer:
            self.cat.delete(layer)

        payload = {
            "base_file": open(self.valid_geojson, "rb"),
        }
        initial_name = "valid"
        self._assertimport(payload, initial_name)
        layer = self.cat.get_layer("geonode:valid")
        if layer:
            self.cat.delete(layer)

    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(
        GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data"
    )
    def test_import_geojson_overwrite(self):
        prev_dataset = create_single_dataset(name="valid")

        layer = self.cat.get_layer("geonode:valid")
        if layer:
            self.cat.delete(layer)
        payload = {
            "base_file": open(self.valid_geojson, "rb"),
        }
        initial_name = "valid"
        payload["overwrite_existing_layer"] = True
        self._assertimport(
            payload, initial_name, overwrite=True, last_update=prev_dataset.last_updated
        )
        layer = self.cat.get_layer("geonode:valid")
        if layer:
            self.cat.delete(layer)


class ImporterKMLImportTest(BaseImporterEndToEndTest):
    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(
        GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data"
    )
    def test_import_kml(self):
        layer = self.cat.get_layer("geonode:sample_point_dataset")
        if layer:
            self.cat.delete(layer)
        payload = {
            "base_file": open(self.valid_kml, "rb"),
        }
        initial_name = "sample_point_dataset"
        self._assertimport(payload, initial_name)
        layer = self.cat.get_layer("geonode:sample_point_dataset")
        if layer:
            self.cat.delete(layer)

    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(
        GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data"
    )
    def test_import_kml_overwrite(self):
        prev_dataset = create_single_dataset(name="sample_point_dataset")

        layer = self.cat.get_layer("geonode:sample_point_dataset")
        if layer:
            self.cat.delete(layer)
        payload = {
            "base_file": open(self.valid_kml, "rb"),
        }
        initial_name = "sample_point_dataset"
        payload["overwrite_existing_layer"] = True
        self._assertimport(
            payload, initial_name, overwrite=True, last_update=prev_dataset.last_updated
        )
        layer = self.cat.get_layer("geonode:sample_point_dataset")
        if layer:
            self.cat.delete(layer)


class ImporterShapefileImportTest(BaseImporterEndToEndTest):
    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(
        GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data"
    )
    def test_import_shapefile(self):
        layer = self.cat.get_layer("geonode:san_andres_y_providencia_highway")
        if layer:
            self.cat.delete(layer)
        payload = {
            _filename: open(_file, "rb") for _filename, _file in self.valid_shp.items()
        }
        initial_name = "san_andres_y_providencia_highway"
        self._assertimport(payload, initial_name)
        if layer:
            layer = self.cat.get_layer("geonode:san_andres_y_providencia_highway")
            self.cat.delete(layer)

    @mock.patch.dict(os.environ, {"GEONODE_GEODATABASE": "test_geonode_data"})
    @override_settings(
        GEODATABASE_URL=f"{geourl.split('/geonode_data')[0]}/test_geonode_data"
    )
    def test_import_shapefile_overwrite(self):

        prev_dataset = create_single_dataset(name="san_andres_y_providencia_highway")

        layer = self.cat.get_layer("geonode:san_andres_y_providencia_highway")
        if layer:
            self.cat.delete(layer)
        payload = {
            _filename: open(_file, "rb") for _filename, _file in self.valid_shp.items()
        }
        initial_name = "san_andres_y_providencia_highway"
        payload["overwrite_existing_layer"] = True
        self._assertimport(
            payload, initial_name, overwrite=True, last_update=prev_dataset.last_updated
        )
        layer = self.cat.get_layer("geonode:san_andres_y_providencia_highway")
        if layer:
            self.cat.delete(layer)
