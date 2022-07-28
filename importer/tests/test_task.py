from django.contrib.auth import get_user_model
from geonode.tests.base import GeoNodeBaseTestSupport
from unittest.mock import patch
from importer.api.exception import InvalidInputFileException, PublishResourceException

from importer.celery_tasks import create_geonode_resource, import_orchestrator, import_resource, orchestrator, publish_resource
from geonode.resource.models import ExecutionRequest
from geonode.layers.models import Dataset
from geonode.resource.enumerator import ExecutionRequestAction
# Create your tests here.


class TestCeleryTasks(GeoNodeBaseTestSupport):

    @patch("importer.celery_tasks.orchestrator.perform_next_step")
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


    @patch("importer.celery_tasks.orchestrator.perform_next_step")
    @patch("importer.celery_tasks.DataStoreManager.input_is_valid")
    def test_import_resource_should_rase_exp_if_is_invalid(
        self, is_valid, importer,
    ):  
        user = get_user_model().objects.first()
    
        exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=user),
                func_name="dummy_func",
                step="dummy_step",
                legacy_upload_name="dummy",
                input_params={
                    "files": "/filepath",
                    "store_spatial_files": True
                }
        )

        is_valid.side_effect = Exception(f"Invalid format type")

        with self.assertRaises(InvalidInputFileException) as _exc:
            import_resource(
                str(exec_id),
                action=ExecutionRequestAction.IMPORT.value,
                handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler'
            )
        expected_msg = f"Invalid format type {str(exec_id)}"
        self.assertEqual(expected_msg, str(_exc.exception.detail))
        ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()
    
    
    @patch("importer.celery_tasks.orchestrator.perform_next_step")
    @patch("importer.celery_tasks.DataStoreManager.input_is_valid")
    @patch("importer.celery_tasks.DataStoreManager.start_import")
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
                }
        )

        import_resource(
            str(exec_id),
            resource_type='gpkg',
            action=ExecutionRequestAction.IMPORT.value,
            handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler'
        )

        start_import.assert_called_once()
        ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    @patch("importer.celery_tasks.DataPublisher.extract_resource_to_publish")
    @patch("importer.celery_tasks.DataPublisher.publish_resources")
    def test_publish_resource_should_work(
        self, publish_resources, extract_resource_to_publish, importer,
    ):
        try:
            publish_resources.return_value = True
            extract_resource_to_publish.return_value = [
                {"crs": 12345, "name": "dataset3"}
            ]

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

            publish_resource(
                str(exec_id),
                resource_type='gpkg',
                step_name="publish_resource",
                layer_name="dataset3",
                alternate="alternate_dataset3",
                action=ExecutionRequestAction.IMPORT.value,
                handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler'
            )

            # Evaluation
            req = ExecutionRequest.objects.get(exec_id=str(exec_id))
            self.assertEqual(publish_resources.call_count , 1)
            self.assertEqual('importer.publish_resource', req.step)
            importer.assert_called_once()
        finally:
            #cleanup
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    @patch("importer.celery_tasks.DataPublisher.extract_resource_to_publish")
    @patch("importer.celery_tasks.DataPublisher.publish_resources")
    def test_publish_resource_if_overwrite_should_not_call_the_publishing(
        self, publish_resources, extract_resource_to_publish, importer,
    ):
        try:
            publish_resources.return_value = True
            extract_resource_to_publish.return_value = [
                {"crs": 12345, "name": "dataset3"}
            ]

            user = get_user_model().objects.first()
        
            exec_id = orchestrator.create_execution_request(
                    user=get_user_model().objects.get(username=user),
                    func_name="dummy_func",
                    step="dummy_step",
                    legacy_upload_name="dummy",
                    input_params={
                        "files": "/filepath",
                        "override_existing_layer": True,
                        "store_spatial_files": True
                    },
            )

            publish_resource(
                str(exec_id),
                resource_type='gpkg',
                step_name="publish_resource",
                layer_name="dataset3",
                alternate="alternate_dataset3"
            )

            # Evaluation
            req = ExecutionRequest.objects.get(exec_id=str(exec_id))
            self.assertEqual('importer.publish_resource', req.step)            
            publish_resources.assert_not_called()
            importer.assert_called_once()

        finally:
            #cleanup
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    def test_create_geonode_resource(self, import_orchestrator):
        try:
            user = get_user_model().objects.first()

            exec_id = orchestrator.create_execution_request(
                    user=get_user_model().objects.get(username=user),
                    func_name="dummy_func",
                    step="dummy_step",
                    legacy_upload_name="dummy",
                    input_params={
                        "files": {"base_file": "/filepath"},
                        #"override_existing_layer": True,
                        "store_spatial_files": True
                    },
            )
            alternate = "geonode:alternate_foo_dataset"
            self.assertFalse(Dataset.objects.filter(alternate=alternate).exists())

            create_geonode_resource(
                str(exec_id),
                resource_type='gpkg',
                step_name="create_geonode_resource",
                layer_name="foo_dataset",
                alternate="alternate_foo_dataset",
                handler_module_path='importer.handlers.gpkg.handler.GPKGFileHandler',
                action='import'
            )


            # Evaluation
            req = ExecutionRequest.objects.get(exec_id=str(exec_id))
            self.assertEqual('importer.create_geonode_resource', req.step)            

            self.assertTrue(Dataset.objects.filter(alternate=alternate).exists())

            import_orchestrator.assert_called_once()

        finally:
            #cleanup
            if Dataset.objects.filter(alternate=alternate).exists():
                Dataset.objects.filter(alternate=alternate).delete()
