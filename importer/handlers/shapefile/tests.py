
import os

import gisdata
from django.contrib.auth import get_user_model
from django.test import TestCase
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.upload.models import UploadParallelismLimit
from importer import project_dir
from importer.handlers.shapefile.handler import ShapeFileHandler
from osgeo import ogr

from importer.handlers.shapefile.serializer import ShapeFileSerializer


class TestShapeFileFileHandler(TestCase):
    databases = ("default", "datastore")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.handler = ShapeFileHandler()
        file_path = gisdata.VECTOR_DATA
        filename = os.path.join(file_path, "san_andres_y_providencia_highway.shp")
        cls.valid_shp = {
            "base_file": filename,
            "dbf_file": f"{file_path}/san_andres_y_providencia_highway.dbf",
            "prj_file": f"{file_path}/san_andres_y_providencia_highway.prj",
            "shx_file": f"{file_path}/san_andres_y_providencia_highway.shx",
        }
        cls.invalid_shp = f"{project_dir}/tests/fixture/invalid.geojson"
        cls.user, _ = get_user_model().objects.get_or_create(username="admin")
        cls.invalid_files = {"base_file": cls.invalid_shp}
        cls.owner = get_user_model().objects.first()


    def test_task_list_is_the_expected_one(self):
        expected = (
            "start_import",
            "importer.import_resource",
            "importer.publish_resource",
            "importer.create_geonode_resource"
        )
        self.assertEqual(len(self.handler.ACTIONS['import']), 4)
        self.assertTupleEqual(expected, self.handler.ACTIONS['import'])


    def test_task_list_is_the_expected_one(self):
        expected = (
            "start_copy",
            "importer.copy_geonode_resource",
            "importer.copy_dynamic_model",
            "importer.copy_geonode_data_table",
            "importer.publish_resource"
        )
        self.assertEqual(len(self.handler.ACTIONS['copy']), 5)
        self.assertTupleEqual(expected, self.handler.ACTIONS['copy'])

    def test_is_valid_should_raise_exception_if_the_parallelism_is_met(self):
        parallelism, created = UploadParallelismLimit.objects.get_or_create(slug="default_max_parallel_uploads")
        old_value = parallelism.max_number
        try:
            if not created:
                UploadParallelismLimit.objects.filter(slug="default_max_parallel_uploads").update(max_number=0)

            with self.assertRaises(UploadParallelismLimitException) as _exc:
                self.handler.is_valid(files=self.valid_shp, user=self.user)
            
        finally:
            parallelism.max_number = old_value
            parallelism.save()

    def test_is_valid_should_pass_with_valid_shp(self):
        self.handler.is_valid(files=self.valid_shp, user=self.user)

    def test_get_ogr2ogr_driver_should_return_the_expected_driver(self):
        expected = ogr.GetDriverByName("ESRI Shapefile")
        actual = self.handler.get_ogr2ogr_driver()
        self.assertEqual(type(expected), type(actual))

    def test_can_handle_should_return_true_for_shp(self):
        actual = self.handler.can_handle(self.valid_shp)
        self.assertTrue(actual)

    def test_can_handle_should_return_false_for_other_files(self):
        actual = self.handler.can_handle({"base_file": "random.prj"})
        self.assertFalse(actual)

    def test_should_get_the_specific_serializer(self):
        actual = self.handler.has_serializer(self.valid_shp)
        self.assertEqual(type(actual), type(ShapeFileSerializer))

    def test_should_NOT_get_the_specific_serializer(self):
        actual = self.handler.has_serializer(self.invalid_files)
        self.assertFalse(actual)
