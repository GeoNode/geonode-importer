from django.contrib.auth import get_user_model
from geonode.tests.base import GeoNodeBaseTestSupport
from unittest.mock import patch
from importer.api.exception import InvalidInputFileException

from importer.views import import_orchestrator, import_resource, orchestrator
from geonode.resource.models import ExecutionRequest

# Create your tests here.


class TestCeleryTasks(GeoNodeBaseTestSupport):
    @patch("importer.views.orchestrator.perform_next_import_step")
    def test_import_orchestrator_create_exececution_request_if_none(self, importer):
        user = get_user_model().objects.first()
        count = ExecutionRequest.objects.count()

        import_orchestrator(
            files={"base_file": "/tmp/file.gpkg"},
            store_spatial_files=True,
            user=user.username,
            execution_id=None,
        )

        self.assertEqual(count + 1, ExecutionRequest.objects.count())
        importer.assert_called_once()

    @patch("importer.views.orchestrator.perform_next_import_step")
    def test_import_orchestrator_dont_create_exececution_request_if_not__none(
        self, importer
    ):
        user = get_user_model().objects.first()
        count = ExecutionRequest.objects.count()

        import_orchestrator(
            files={"base_file": "/tmp/file.gpkg"},
            store_spatial_files=True,
            user=user.username,
            execution_id="some value",
        )

        self.assertEqual(count, ExecutionRequest.objects.count())
        importer.assert_called_once()


    @patch("importer.views.orchestrator.perform_next_import_step")
    @patch("importer.views.DataStoreManager.input_is_valid")
    def test_import_resource_should_rase_exp_if_is_invalid(
        self, is_valid, importer,
    ):  
        is_valid.side_effect = Exception("Invalid format type")
        user = get_user_model().objects.first()
    
        exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=user),
                func_name="dummy_func",
                step="dummy_step",
                legacy_upload_name="dummy",
                input_params={
                    "files": "/filepath",
                    "store_spatial_files": True
                },
        )

        with self.assertRaises(InvalidInputFileException) as _exc:
            import_resource(
                str(exec_id),
                resource_type='gpkg'
            )
        expected_msg = "Invalid format type"
        self.assertEqual(expected_msg, str(_exc.exception.detail))
        ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()
    
    
    @patch("importer.views.orchestrator.perform_next_import_step")
    @patch("importer.views.DataStoreManager.input_is_valid")
    @patch("importer.views.DataStoreManager.start_import")
    def test_import_resource_should_work(
        self, start_import, is_valid, importer,
    ):  
        is_valid.return_value = True
        user = get_user_model().objects.first()
    
        exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=user),
                func_name="dummy_func",
                step="dummy_step",
                legacy_upload_name="dummy",
                input_params={
                    "files": "/filepath",
                    "store_spatial_files": True
                },
        )

        import_resource(
            str(exec_id),
            resource_type='gpkg'
        )

        start_import.assert_called_once()