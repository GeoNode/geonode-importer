import uuid
from django.contrib.auth import get_user_model
from geonode.tests.base import GeoNodeBaseTestSupport
from unittest.mock import patch
from importer.api.exception import ImportException
from importer.handlers.base import AbstractHandler
from importer.handlers.gpkg.handler import GPKGFileHandler
from importer.orchestrator import SUPPORTED_TYPES, ImportOrchestrator
from geonode.upload.models import Upload
from django.utils import timezone

from geonode.base import enumerations as enum
from geonode.resource.models import ExecutionRequest

# Create your tests here.


class TestsImporterOrchestrator(GeoNodeBaseTestSupport):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.orchestrator = ImportOrchestrator()

    def test_get_supported_type_list(self):
        actual = self.orchestrator.supported_type
        self.assertEqual(SUPPORTED_TYPES.keys(), actual)

    @patch("importer.orchestrator.SUPPORTED_TYPES")
    def test_get_file_handler_raise_error_if_not_exists(self, supported_types):
        supported_types.get.return_value = {}
        with self.assertRaises(ImportException) as _exc:
            self.orchestrator.get_file_handler("invalid_type")
        self.assertEqual(
            str(_exc.exception.detail),
            "The requested filetype is not supported: invalid_type",
        )

    @patch("importer.orchestrator.SUPPORTED_TYPES")
    def test_get_file_handler(self, supported_types):
        supported_types.get.return_value = {"gpkg": GPKGFileHandler()}
        actual = self.orchestrator.get_file_handler("gpkg")
        self.assertIsInstance(actual.get("gpkg"), AbstractHandler)

    def test_get_execution_object_raise_exp_if_not_exists(self):
        with self.assertRaises(ImportException) as _exc:
            self.orchestrator.get_execution_object(str(uuid.uuid4()))

        self.assertEqual(
            str(_exc.exception.detail), "The selected UUID does not exists"
        )

    def test_get_execution_object_retrun_exp(self):
        _uuid = str(uuid.uuid4())
        ExecutionRequest.objects.create(exec_id=_uuid, func_name="test")
        try:
            _exec = self.orchestrator.get_execution_object(_uuid)
            self.assertIsNotNone(_exec)
        finally:
            ExecutionRequest.objects.filter(exec_id=_uuid).delete()

    def test_create_execution_request(self):
        handler = self.orchestrator.get_file_handler("gpkg")
        count = ExecutionRequest.objects.count()
        input_files = {
            "files": {"base_file": "/tmp/file.txt"},
            "store_spatial_files": True,
        }
        exec_id = self.orchestrator.create_execution_request(
            user=get_user_model().objects.first(),
            func_name=next(iter(handler.TASKS_LIST)),
            step=next(iter(handler.TASKS_LIST)),
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True,
            },
        )
        exec_obj = ExecutionRequest.objects.filter(exec_id=exec_id).first()
        self.assertEqual(count + 1, ExecutionRequest.objects.count())
        self.assertDictEqual(input_files, exec_obj.input_params)
        self.assertEqual(exec_obj.STATUS_READY, exec_obj.status)
        # check that also the legacy is created
        self.assertIsNotNone(Upload.objects.get(metadata__icontains=exec_id))

    @patch("importer.orchestrator.importer_app.tasks.get")
    def test_perform_next_import_step(self, mock_celery):
        # setup test
        handler = self.orchestrator.get_file_handler("gpkg")
        _id = self.orchestrator.create_execution_request(
            user=get_user_model().objects.first(),
            func_name=next(iter(handler.TASKS_LIST)),
            step="start_import",  # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True,
            },
        )
        # test under tests
        self.orchestrator.perform_next_import_step("gpkg", _id)
        mock_celery.assert_called_once()
        mock_celery.assert_called_with("importer.import_resource")

    @patch("importer.orchestrator.importer_app.tasks.get")
    def test_perform_last_import_step(self, mock_celery):
        # setup test
        handler = self.orchestrator.get_file_handler("gpkg")
        _id = self.orchestrator.create_execution_request(
            user=get_user_model().objects.first(),
            func_name=next(iter(handler.TASKS_LIST)),
            step="importer.create_gn_resource",  # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True,
            },
        )
        # test under tests
        self.orchestrator.perform_next_import_step("gpkg", _id)
        mock_celery.assert_not_called()

    @patch("importer.orchestrator.importer_app.tasks.get")
    def test_perform_with_error_set_invalid_status(self, mock_celery):
        mock_celery.side_effect = Exception("test exception")
        # setup test
        handler = self.orchestrator.get_file_handler("gpkg")
        _id = self.orchestrator.create_execution_request(
            user=get_user_model().objects.first(),
            func_name=next(iter(handler.TASKS_LIST)),
            step="start_import",  # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True,
            },
        )
        # test under tests
        with self.assertRaises(Exception):
            self.orchestrator.perform_next_import_step("gpkg", _id)

        _excec = ExecutionRequest.objects.filter(exec_id=_id).first()
        self.assertIsNotNone(_excec)
        self.assertEqual(ExecutionRequest.STATUS_FAILED, _excec.status)
        self.assertIsNotNone(Upload.objects.get(metadata__icontains=_id))

    def test_set_as_failed(self):
        # we need to create first the execution
        _uuid = self.orchestrator.create_execution_request(
            user=get_user_model().objects.first(),
            func_name="name",
            step="importer.create_gn_resource",  # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True,
            },
        )
        _uuid = str(_uuid)
        self.orchestrator.set_as_failed(_uuid, reason="automatic test")

        #check normal execution status
        req = ExecutionRequest.objects.get(exec_id=_uuid)
        self.assertTrue(req.status, ExecutionRequest.STATUS_FAILED)
        self.assertTrue(req.log, "automatic test")

        #check legacy execution status
        legacy = Upload.objects.filter(metadata__contains=_uuid)
        self.assertTrue(legacy.exists())
        self.assertEqual(legacy.first().state, enum.STATE_INVALID)

        #cleanup
        req.delete()
        legacy.delete()

    def test_set_as_completed(self):
        # we need to create first the execution
        _uuid = self.orchestrator.create_execution_request(
            user=get_user_model().objects.first(),
            func_name="name",
            step="importer.create_gn_resource",  # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True,
            },
        )

        # calling the function
        self.orchestrator.set_as_completed(_uuid)

        req = ExecutionRequest.objects.get(exec_id=_uuid)
        self.assertTrue(req.status, ExecutionRequest.STATUS_FINISHED)

        #check legacy execution status
        legacy = Upload.objects.filter(metadata__contains=_uuid)
        self.assertTrue(legacy.exists())
        self.assertEqual(legacy.first().state, enum.STATE_PROCESSED)

        #cleanup
        req.delete()
        legacy.delete()

    def test_update_execution_request_status(self):
        # we need to create first the execution
        _uuid = self.orchestrator.create_execution_request(
            user=get_user_model().objects.first(),
            func_name="name",
            step="importer.create_gn_resource",  # adding the first step for the GPKG file
            input_params={
                "files": {"base_file": "/tmp/file.txt"},
                "store_spatial_files": True,
            },
        )

        self.orchestrator.update_execution_request_status(
            execution_id=_uuid,
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="function_name",
            step="step_here",
        )
        req = ExecutionRequest.objects.get(exec_id=_uuid)
        self.assertTrue(req.status, ExecutionRequest.STATUS_RUNNING)
        self.assertTrue(req.func_name, "function_name")
        self.assertTrue(req.step, "step_here")

        #check legacy execution status
        legacy = Upload.objects.filter(metadata__contains=_uuid)
        self.assertTrue(legacy.exists())
        self.assertEqual(legacy.first().state, enum.STATE_RUNNING)

        #cleanup
        req.delete()
        legacy.delete()

