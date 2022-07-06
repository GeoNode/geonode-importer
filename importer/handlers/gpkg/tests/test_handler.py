
from django.test import TestCase
from importer.handlers.gpkg.exceptions import InvalidGeopackageException
from django.contrib.auth import get_user_model
from importer.handlers.gpkg.handler import GPKGFileHandler
from importer import project_dir
from geonode.upload.models import Upload, UploadSizeLimit, UploadParallelismLimit
from geonode.upload.api.exceptions import UploadParallelismLimitException, FileUploadLimitException


class TestGPKGHandler(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.handler = GPKGFileHandler()
        cls.valid_gpkg = f"{project_dir}/tests/fixture/valid.gpkg"
        cls.invalid_gpkg = f"{project_dir}/tests/fixture/invalid.gpkg"
        cls.user, _ = get_user_model().objects.get_or_create(username="admin")
        cls.invalid_files = {"base_file": cls.invalid_gpkg}
        cls.valid_files = {"base_file": cls.valid_gpkg}

    def test_task_list_is_the_expected_one(self):
        expected = (
            "start_import",
            "importer.import_resource",
            "importer.publish_resource",
            "importer.create_gn_resource"
        )
        self.assertEqual(len(self.handler.TASKS_LIST), 4)
        self.assertTupleEqual(expected, self.handler.TASKS_LIST)

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


    def test_create_error_log(self):
        '''
        Should return the formatted way for the log of the handler
        '''
        actual = self.handler.create_error_log(
            Exception("my exception"),
            "foo_task_name",
            *["exec_id", "layer_name", "alternate"]
        )
        expected = f"Task: foo_task_name raised an error during actions for layer: alternate: my exception"
        self.assertEqual(expected, actual)