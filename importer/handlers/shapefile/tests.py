import os
import uuid

import gisdata
from django.contrib.auth import get_user_model
from django.test import TestCase
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.upload.models import UploadParallelismLimit
from mock import MagicMock, patch, mock_open
from importer import project_dir
from importer.handlers.common.vector import import_with_ogr2ogr
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
            "importer.create_geonode_resource",
        )
        self.assertEqual(len(self.handler.ACTIONS["import"]), 4)
        self.assertTupleEqual(expected, self.handler.ACTIONS["import"])

    def test_copy_task_list_is_the_expected_one(self):
        expected = (
            "start_copy",
            "importer.copy_dynamic_model",
            "importer.copy_geonode_data_table",
            "importer.publish_resource",
            "importer.copy_geonode_resource",
        )
        self.assertEqual(len(self.handler.ACTIONS["copy"]), 5)
        self.assertTupleEqual(expected, self.handler.ACTIONS["copy"])

    def test_is_valid_should_raise_exception_if_the_parallelism_is_met(self):
        parallelism, created = UploadParallelismLimit.objects.get_or_create(
            slug="default_max_parallel_uploads"
        )
        old_value = parallelism.max_number
        try:
            UploadParallelismLimit.objects.filter(
                slug="default_max_parallel_uploads"
            ).update(max_number=0)

            with self.assertRaises(UploadParallelismLimitException):
                self.handler.is_valid(files=self.valid_shp, user=self.user)
        finally:
            parallelism.max_number = old_value
            parallelism.save()

    def test_promote_to_multi(self):
        # point should be keep as point
        actual = self.handler.promote_to_multi("Point")
        self.assertEqual("Point", actual)
        # polygon should be changed into multipolygon
        actual = self.handler.promote_to_multi("Polygon")
        self.assertEqual("Multi Polygon", actual)

        # linestring should be changed into multilinestring
        actual = self.handler.promote_to_multi("Linestring")
        self.assertEqual("Multi Linestring", actual)

        # if is already multi should be kept
        actual = self.handler.promote_to_multi("Multi Point")
        self.assertEqual("Multi Point", actual)

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

    def test_should_create_ogr2ogr_command_with_encoding_from_cst(self):
        shp_with_cst = self.valid_shp.copy()
        cst_file = self.valid_shp["base_file"].replace("shp", "cst")
        shp_with_cst["cst_file"] = cst_file
        patch_location = "importer.handlers.shapefile.handler.open"
        with patch(patch_location, new=mock_open(read_data="UTF-8")) as _file:
            actual = self.handler.create_ogr2ogr_command(shp_with_cst, "a", False, "a")

            _file.assert_called_once_with(cst_file, "r")
            self.assertIn("--config SHAPE_ENCODING UTF-8", actual)

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
            files=self.valid_shp,
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
            "/usr/bin/ogr2ogr --config PG_USE_COPY YES -f PostgreSQL PG:\" dbname='test_geonode_data' host="
            + os.getenv("DATABASE_HOST", "localhost")
            + " port=5432 user='geonode_data' password='geonode_data' \" \""
            + self.valid_shp.get("base_file")
            + '" -nln alternate "dataset" -lco precision=no -lco GEOMETRY_NAME=geometry ',
            stdout=-1,
            stderr=-1,
            shell=True,  # noqa
        )
