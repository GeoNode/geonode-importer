
from django.test import TestCase
from importer.handlers.gpkg.exceptions import InvalidGeopackageException
from django.contrib.auth import get_user_model
from importer.handlers.gpkg.handler import GPKGFileHandler
from importer import project_dir
from geonode.upload.models import UploadParallelismLimit
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.base.populate_test_data import create_single_dataset
from osgeo import ogr

class TestGPKGHandler(TestCase):
    databases = ("default", "datastore")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.handler = GPKGFileHandler()
        cls.valid_gpkg = f"{project_dir}/tests/fixture/valid.gpkg"
        cls.invalid_gpkg = f"{project_dir}/tests/fixture/invalid.gpkg"
        cls.user, _ = get_user_model().objects.get_or_create(username="admin")
        cls.invalid_files = {"base_file": cls.invalid_gpkg}
        cls.valid_files = {"base_file": cls.valid_gpkg}
        cls.owner = get_user_model().objects.first()
        cls.layer = create_single_dataset(name='stazioni_metropolitana', owner=cls.owner)


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

    def test_is_valid_should_raise_exception_if_the_gpkg_is_invalid(self):
        with self.assertRaises(InvalidGeopackageException) as _exc:
            self.handler.is_valid(files=self.invalid_files, user=self.user)
        
        self.assertIsNotNone(_exc)
        self.assertTrue(
            "Layer names must start with a letter, and valid characters are lowercase a-z, numbers or underscores" in str(_exc.exception.detail)
        )

    def test_is_valid_should_raise_exception_if_the_parallelism_is_met(self):
        parallelism, created = UploadParallelismLimit.objects.get_or_create(slug="default_max_parallel_uploads")
        old_value = parallelism.max_number
        try:
            if not created:
                UploadParallelismLimit.objects.filter(slug="default_max_parallel_uploads").update(max_number=0)

            with self.assertRaises(UploadParallelismLimitException) as _exc:
                self.handler.is_valid(files=self.valid_files, user=self.user)
            
        finally:
            parallelism.max_number = old_value
            parallelism.save()


    def test_is_valid_should_raise_exception_if_layer_are_greater_than_max_parallel_upload(self):
        parallelism, created = UploadParallelismLimit.objects.get_or_create(slug="default_max_parallel_uploads")
        old_value = parallelism.max_number
        try:
            if not created:
                UploadParallelismLimit.objects.filter(slug="default_max_parallel_uploads").update(max_number=1)

            with self.assertRaises(UploadParallelismLimitException) as _exc:
                self.handler.is_valid(files=self.valid_files, user=self.user)
            
        finally:
            parallelism.max_number = old_value
            parallelism.save()


    def test_is_valid_should_pass_with_valid_gpkg(self):
        self.handler.is_valid(files=self.valid_files, user=self.user)

    def test_get_ogr2ogr_driver_should_return_the_expected_driver(self):
        expected = ogr.GetDriverByName("GPKG")
        actual = self.handler.get_ogr2ogr_driver()
        self.assertEqual(type(expected), type(actual))

    def test_can_handle_should_return_true_for_geopackage(self):
        actual = self.handler.can_handle(self.valid_files)
        self.assertTrue(actual)

    def test_can_handle_should_return_false_for_other_files(self):
        actual = self.handler.can_handle({"base_file": "random.file"})
        self.assertFalse(actual)
