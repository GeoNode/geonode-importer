from django.contrib.auth import get_user_model
from geonode.tests.base import GeoNodeBaseTestSupport
from unittest.mock import patch
from importer.api.exception import InvalidInputFileException, PublishResourceException

from importer.celery_tasks import create_gn_resource, import_orchestrator, import_resource, orchestrator, publish_resource
from geonode.resource.models import ExecutionRequest
from geonode.layers.models import Dataset
# Create your tests here.


class TestCeleryTasks(GeoNodeBaseTestSupport):
    @patch("importer.celery_tasks.orchestrator.perform_next_import_step")
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

    @patch("importer.celery_tasks.orchestrator.perform_next_import_step")
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


    @patch("importer.celery_tasks.orchestrator.perform_next_import_step")
    @patch("importer.celery_tasks.DataStoreManager.input_is_valid")
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
    
    
    @patch("importer.celery_tasks.orchestrator.perform_next_import_step")
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
                },
        )

        import_resource(
            str(exec_id),
            resource_type='gpkg'
        )

        start_import.assert_called_once()
        ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    @patch("importer.celery_tasks.DataPublisher.extract_resource_name_and_crs")
    @patch("importer.celery_tasks.DataPublisher.publish_resources")
    def test_publish_resource_should_work(
        self, publish_resources, extract_resource_name_and_crs, importer,
    ):
        try:
            publish_resources.return_value = True, "workspace", "store"
            extract_resource_name_and_crs.return_value = [
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
                alternate="alternate_dataset3"
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
    @patch("importer.celery_tasks.DataPublisher.extract_resource_name_and_crs")
    @patch("importer.celery_tasks.DataPublisher.publish_resources")
    def test_publish_resource_should_not_call_the_publishing_if_crs_is_not_provided(
        self, publish_resources, extract_resource_name_and_crs, importer,
    ):
        try:
            publish_resources.return_value = True, "workspace", "store"
            extract_resource_name_and_crs.return_value = [
                {"name": "This should not be published since the CRS is not provided"}
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
            with self.assertRaises(PublishResourceException) as _exc:
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
            expected_msg = "Only resources with a CRS provided can be published"
            self.assertEqual(expected_msg, str(_exc.exception.detail))
            
            publish_resources.assert_not_called()
            importer.assert_not_called()
        finally:
            #cleanup
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=str(exec_id)).delete()

    @patch("importer.celery_tasks.import_orchestrator.apply_async")
    @patch("importer.celery_tasks.DataPublisher.extract_resource_name_and_crs")
    @patch("importer.celery_tasks.DataPublisher.publish_resources")
    def test_publish_resource_if_overwrite_should_not_call_the_publishing(
        self, publish_resources, extract_resource_name_and_crs, importer,
    ):
        try:
            publish_resources.return_value = True, "workspace", "store"
            extract_resource_name_and_crs.return_value = [
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
    def test_create_gn_resource(self, import_orchestrator):
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

            create_gn_resource(
                str(exec_id),
                resource_type='gpkg',
                step_name="create_gn_resource",
                layer_name="foo_dataset",
                alternate="alternate_foo_dataset"
            )


            # Evaluation
            req = ExecutionRequest.objects.get(exec_id=str(exec_id))
            self.assertEqual('importer.create_gn_resource', req.step)            

            self.assertTrue(Dataset.objects.filter(alternate=alternate).exists())

            import_orchestrator.assert_called_once()

        finally:
            #cleanup
            if Dataset.objects.filter(alternate=alternate).exists():
                Dataset.objects.filter(alternate=alternate).delete()
