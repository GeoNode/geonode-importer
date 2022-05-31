import tastypie.compat
import uuid
import PIL
from django.contrib.auth import get_user_model
from django.test import SimpleTestCase
from geonode.tests.base import GeoNodeBaseTestSupport
from django.urls import reverse
from unittest.mock import MagicMock, patch
from importer.api.exception import ImportException
from importer.handlers import AbstractHandler, GPKGFileHandler
from importer.orchestrator import SUPPORTED_TYPES, ImportOrchestrator

from importer.views import import_orchestrator
from geonode.resource.models import ExecutionRequest
# Create your tests here.


class TestCeleryTasks(GeoNodeBaseTestSupport):

    @patch("importer.views.importer.perform_next_import_step")
    def test_import_orchestrator_create_exececution_request_if_none(self, importer):
        user = get_user_model().objects.first()
        count = ExecutionRequest.objects.count()

        import_orchestrator(
            files={"base_file": "/tmp/file.txt"},
            store_spatial_files=True,
            user=user.username,
            execution_id=None
        )

        self.assertEqual(count+1, ExecutionRequest.objects.count())
        importer.assert_called_once()


    @patch("importer.views.importer.perform_next_import_step")
    def test_import_orchestrator_dont_create_exececution_request_if_not__none(self, importer):
        user = get_user_model().objects.first()
        count = ExecutionRequest.objects.count()

        import_orchestrator(
            files={"base_file": "/tmp/file.txt"},
            store_spatial_files=True,
            user=user.username,
            execution_id="some value"
        )

        self.assertEqual(count, ExecutionRequest.objects.count())
        importer.assert_called_once()


class TestsImporterOrchestrator(GeoNodeBaseTestSupport):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.importer = ImportOrchestrator()
    
    def test_get_supported_type_list(self):
        actual = self.importer.supported_type
        self.assertEqual(SUPPORTED_TYPES.keys(), actual)

    @patch("importer.orchestrator.SUPPORTED_TYPES")
    def test_get_file_handler_raise_error_if_not_exists(self, supported_types):
        supported_types.get.return_value = {}
        with self.assertRaises(ImportException) as _exc:
            self.importer.get_file_handler('invalid_type')
        self.assertEqual(str(_exc.exception.detail), "The requested filetype is not supported: invalid_type")

    @patch("importer.orchestrator.SUPPORTED_TYPES")
    def test_get_file_handler(self, supported_types):
        supported_types.get.return_value = {
            "gpkg": GPKGFileHandler()
        }
        actual = self.importer.get_file_handler('gpkg')
        self.assertIsInstance(actual.get('gpkg'), AbstractHandler)

    def test_get_execution_object_raise_exp_if_not_exists(self):
        with self.assertRaises(ImportException) as _exc:
            self.importer.get_execution_object(str(uuid.uuid4()))
        
        self.assertEqual(str(_exc.exception.detail), "The selected UUID does not exists")

    def test_get_execution_object_retrun_exp(self):
        _uuid = str(uuid.uuid4())
        ExecutionRequest.objects.create(
            exec_id=_uuid,
            func_name="test"
        )
        try:
            _exec = self.importer.get_execution_object(_uuid)
            self.assertIsNotNone(_exec)
        finally:
            ExecutionRequest.objects.filter(exec_id=_uuid).delete()

    def test_create_execution_request(self):
        handler = self.importer.get_file_handler('gpkg')
        count = ExecutionRequest.objects.count()
        input_files = {
            "files": {"base_file": "/tmp/file.txt"},
            "store_spatial_files": True
        }
        exec_id = self.importer.create_execution_request(
            user=get_user_model().objects.first(),
            func_name=next(iter(handler.TASKS_LIST)),
            step=next(iter(handler.TASKS_LIST)),
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True
            }
        )
        exec_obj = ExecutionRequest.objects.filter(exec_id=exec_id).first()
        self.assertEqual(count+1, ExecutionRequest.objects.count())
        self.assertDictEqual(input_files, exec_obj.input_params)
        self.assertEqual(exec_obj.STATUS_READY, exec_obj.status)

    @patch("importer.orchestrator.app.tasks.get")
    def test_perform_next_import_step(self, mock_celery):
        # setup test
        handler = self.importer.get_file_handler('gpkg')
        _id = self.importer.create_execution_request(
            user=get_user_model().objects.first(),
            func_name=next(iter(handler.TASKS_LIST)),
            step="start_import", # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True
            }
        )
        # test under tests
        self.importer.perform_next_import_step('gpkg', _id)
        mock_celery.assert_called_once()
        mock_celery.assert_called_with("importer.import_resource")

    @patch("importer.orchestrator.app.tasks.get")
    def test_perform_last_import_step(self, mock_celery):
        # setup test
        handler = self.importer.get_file_handler('gpkg')
        _id = self.importer.create_execution_request(
            user=get_user_model().objects.first(),
            func_name=next(iter(handler.TASKS_LIST)),
            step="importer.create_gn_resource", # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True
            }
        )
        # test under tests
        self.importer.perform_next_import_step('gpkg', _id)
        mock_celery.assert_not_called()

    @patch("importer.orchestrator.app.tasks.get")
    def test_perform_with_error_set_invalid_status(self, mock_celery):
        mock_celery.side_effect = Exception("test exception")
        # setup test
        handler = self.importer.get_file_handler('gpkg')
        _id = self.importer.create_execution_request(
            user=get_user_model().objects.first(),
            func_name=next(iter(handler.TASKS_LIST)),
            step="start_import", # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True
            }
        )
        # test under tests
        with self.assertRaises(Exception):
            self.importer.perform_next_import_step('gpkg', _id)
        
        _excec = ExecutionRequest.objects.filter(exec_id=_id).first()
        self.assertIsNotNone(_excec)
        self.assertEqual(ExecutionRequest.STATUS_FAILED, _excec.status)
