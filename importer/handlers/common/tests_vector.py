import uuid
from celery.canvas import Signature
from celery import group
from django.test import TestCase
from mock import MagicMock, patch
from importer.handlers.common.vector import BaseVectorFileHandler, import_with_ogr2ogr
from django.contrib.auth import get_user_model
from importer import project_dir
from importer.orchestrator import orchestrator
from geonode.base.populate_test_data import create_single_dataset
from geonode.resource.models import ExecutionRequest
from dynamic_models.models import ModelSchema
from osgeo import ogr


class TestBaseVectorFileHandler(TestCase):
    databases = ("default", "datastore")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.handler = BaseVectorFileHandler()
        cls.valid_gpkg = f"{project_dir}/tests/fixture/valid.gpkg"
        cls.invalid_gpkg = f"{project_dir}/tests/fixture/invalid.gpkg"
        cls.user, _ = get_user_model().objects.get_or_create(username="admin")
        cls.invalid_files = {"base_file": cls.invalid_gpkg}
        cls.valid_files = {"base_file": cls.valid_gpkg}
        cls.owner = get_user_model().objects.first()
        cls.layer = create_single_dataset(
            name="stazioni_metropolitana", owner=cls.owner
        )

    def test_create_error_log(self):
        """
        Should return the formatted way for the log of the handler
        """
        actual = self.handler.create_error_log(
            Exception("my exception"),
            "foo_task_name",
            *["exec_id", "layer_name", "alternate"],
        )
        expected = "Task: foo_task_name raised an error during actions for layer: alternate: my exception"
        self.assertEqual(expected, actual)

    def test_create_dynamic_model_fields(self):
        try:
            # Prepare the test
            exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.first(),
                func_name="funct1",
                step="step",
                input_params={"files": self.valid_files, "skip_existing_layer": True},
            )
            schema, _ = ModelSchema.objects.get_or_create(
                name="test_handler", db_name="datastore"
            )
            layers = ogr.Open(self.valid_gpkg)

            # starting the tests
            dynamic_model, celery_group = self.handler.create_dynamic_model_fields(
                layer=[x for x in layers][0],
                dynamic_model_schema=schema,
                overwrite=False,
                execution_id=str(exec_id),
                layer_name="stazioni_metropolitana",
            )

            self.assertIsNotNone(dynamic_model)
            self.assertIsInstance(celery_group, group)
            self.assertEqual(1, len(celery_group.tasks))
            self.assertEqual(
                "importer.create_dynamic_structure", celery_group.tasks[0].name
            )
        finally:
            if schema:
                schema.delete()
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=exec_id).delete()

    def test_setup_dynamic_model_no_dataset_no_modelschema(self):
        self._assert_test_result()

    def test_setup_dynamic_model_no_dataset_no_modelschema_overwrite_true(self):
        self._assert_test_result(overwrite=True)

    def test_setup_dynamic_model_with_dataset_no_modelschema_overwrite_false(self):
        create_single_dataset(name="stazioni_metropolitana", owner=self.user)
        self._assert_test_result(overwrite=False)

    def test_setup_dynamic_model_with_dataset_no_modelschema_overwrite_True(self):
        create_single_dataset(name="stazioni_metropolitana", owner=self.user)
        self._assert_test_result(overwrite=True)

    def test_setup_dynamic_model_no_dataset_with_modelschema_overwrite_false(self):
        ModelSchema.objects.get_or_create(
            name="stazioni_metropolitana", db_name="datastore"
        )
        self._assert_test_result(overwrite=False)

    def test_setup_dynamic_model_with_dataset_with_modelschema_overwrite_false(self):
        create_single_dataset(name="stazioni_metropolitana", owner=self.user)
        ModelSchema.objects.create(
            name="stazioni_metropolitana", db_name="datastore", managed=True
        )
        self._assert_test_result(overwrite=False)

    def _assert_test_result(self, overwrite=False):
        try:
            # Prepare the test
            exec_id = orchestrator.create_execution_request(
                user=self.user,
                func_name="funct1",
                step="step",
                input_params={"files": self.valid_files, "skip_existing_layer": True},
            )

            layers = ogr.Open(self.valid_gpkg)

            # starting the tests
            dynamic_model, layer_name, celery_group = self.handler.setup_dynamic_model(
                layer=[x for x in layers][0],
                execution_id=str(exec_id),
                should_be_overwritten=overwrite,
                username=self.user,
            )

            self.assertIsNotNone(dynamic_model)

            # check if the uuid has been added to the model name
            self.assertIsNotNone(layer_name)

            self.assertIsInstance(celery_group, group)
            self.assertEqual(1, len(celery_group.tasks))
            self.assertEqual(
                "importer.create_dynamic_structure", celery_group.tasks[0].name
            )
        finally:
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=exec_id).delete()

    @patch("importer.handlers.common.vector.BaseVectorFileHandler.get_ogr2ogr_driver")
    @patch("importer.handlers.common.vector.chord")
    def test_import_resource_should_not_be_imported(self, celery_chord, ogr2ogr_driver):
        """
        If the resource exists and should be skept, the celery task
        is not going to be called and the layer is skipped
        """
        exec_id = None
        try:
            # create the executionId
            exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.first(),
                func_name="funct1",
                step="step",
                input_params={"files": self.valid_files, "skip_existing_layer": True},
            )

            # start the resource import
            self.handler.import_resource(
                files=self.valid_files, execution_id=str(exec_id)
            )

            celery_chord.assert_not_called()
        finally:
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=exec_id).delete()

    @patch("importer.handlers.common.vector.BaseVectorFileHandler.get_ogr2ogr_driver")
    @patch("importer.handlers.common.vector.chord")
    def test_import_resource_should_work(self, celery_chord, ogr2ogr_driver):
        try:
            ogr2ogr_driver.return_value = ogr.GetDriverByName("GPKG")
            exec_id = orchestrator.create_execution_request(
                user=get_user_model().objects.first(),
                func_name="funct1",
                step="step",
                input_params={"files": self.valid_files},
            )

            # start the resource import
            self.handler.import_resource(
                files=self.valid_files, execution_id=str(exec_id)
            )

            celery_chord.assert_called_once()
        finally:
            if exec_id:
                ExecutionRequest.objects.filter(exec_id=exec_id).delete()

    def test_get_ogr2ogr_task_group(self):
        _uuid = uuid.uuid4()

        actual = self.handler.get_ogr2ogr_task_group(
            str(_uuid),
            files=self.valid_files,
            layer="dataset",
            should_be_overwritten=True,
            alternate="abc",
        )
        self.assertIsInstance(actual, (Signature,))
        self.assertEqual("importer.import_with_ogr2ogr", actual.task)

    @patch("importer.handlers.common.vector.Popen")
    def test_import_with_ogr2ogr_without_errors_should_call_the_right_command(
        self, _open
    ):
        _uuid = uuid.uuid4()

        comm = MagicMock()
        comm.communicate.return_value = b"", b""
        _open.return_value = comm

        _task, alternate, execution_id = import_with_ogr2ogr(
            execution_id=str(_uuid),
            files=self.valid_files,
            original_name="dataset",
            handler_module_path=str(self.handler),
            ovverwrite_layer=False,
            alternate="alternate",
        )

        self.assertEqual("ogr2ogr", _task)
        self.assertEqual(alternate, "alternate")
        self.assertEqual(str(_uuid), execution_id)

        _open.assert_called_once()
        _open.assert_called_with(
            f'/usr/bin/ogr2ogr --config PG_USE_COPY YES -f PostgreSQL PG:" dbname=\'geonode_data\' host=localhost port=5434 user=\'geonode\' password=\'geonode\' " "{self.valid_files.get("base_file")}" -lco DIM=2 -nln alternate "dataset"', stdout=-1, stderr=-1, shell=True # noqa
        )

    @patch("importer.handlers.common.vector.Popen")
    def test_import_with_ogr2ogr_with_errors_should_raise_exception(self, _open):
        _uuid = uuid.uuid4()

        comm = MagicMock()
        comm.communicate.return_value = b"", b"ERROR: some error here"
        _open.return_value = comm

        with self.assertRaises(Exception):
            import_with_ogr2ogr(
                execution_id=str(_uuid),
                files=self.valid_files,
                original_name="dataset",
                handler_module_path=str(self.handler),
                ovverwrite_layer=False,
                alternate="alternate",
            )

        _open.assert_called_once()
        _open.assert_called_with(
            f'/usr/bin/ogr2ogr --config PG_USE_COPY YES -f PostgreSQL PG:" dbname=\'geonode_data\' host=localhost port=5434 user=\'geonode\' password=\'geonode\' " "{self.valid_files.get("base_file")}" -lco DIM=2 -nln alternate "dataset"', stdout=-1, stderr=-1, shell=True # noqa
        )
