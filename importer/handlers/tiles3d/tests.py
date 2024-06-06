import json
import os
import shutil
from django.test import TestCase
from importer.handlers.tiles3d.exceptions import Invalid3DTilesException
from importer.handlers.tiles3d.handler import Tiles3DFileHandler
from django.contrib.auth import get_user_model
from importer import project_dir
from importer.orchestrator import orchestrator
from geonode.upload.models import UploadParallelismLimit
from geonode.upload.api.exceptions import UploadParallelismLimitException
from geonode.base.populate_test_data import create_single_dataset
from osgeo import ogr
from geonode.assets.handlers import asset_handler_registry


class TestTiles3DFileHandler(TestCase):
    databases = ("default", "datastore")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.handler = Tiles3DFileHandler()
        cls.valid_3dtile = f"{project_dir}/tests/fixture/3dtilesample/valid_3dtiles.zip"
        cls.valid_tileset = f"{project_dir}/tests/fixture/3dtilesample/tileset.json"
        cls.valid_tileset_with_region = (
            f"{project_dir}/tests/fixture/3dtilesample/tileset_with_region.json"
        )
        cls.invalid_tileset = (
            f"{project_dir}/tests/fixture/3dtilesample/invalid_tileset.json"
        )
        cls.invalid_3dtile = f"{project_dir}/tests/fixture/3dtilesample/invalid.zip"
        cls.user, _ = get_user_model().objects.get_or_create(username="admin")
        cls.invalid_files = {"base_file": cls.invalid_3dtile}
        cls.valid_files = {"base_file": cls.valid_3dtile}
        cls.owner = get_user_model().objects.exclude(username="AnonymousUser").first()
        cls.layer = create_single_dataset(
            name="urban_forestry_street_tree_benefits_epsg_26985", owner=cls.owner
        )
        cls.asset_handler = asset_handler_registry.get_default_handler()

    def test_task_list_is_the_expected_one(self):
        expected = (
            "start_import",
            "importer.import_resource",
            "importer.create_geonode_resource",
        )
        self.assertEqual(len(self.handler.ACTIONS["import"]), 3)
        self.assertTupleEqual(expected, self.handler.ACTIONS["import"])

    def test_task_list_is_the_expected_one_copy(self):
        expected = (
            "start_copy",
            "importer.copy_geonode_resource",
        )
        self.assertEqual(len(self.handler.ACTIONS["copy"]), 2)
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
                self.handler.is_valid(files=self.valid_files, user=self.user)

        finally:
            parallelism.max_number = old_value
            parallelism.save()

    def test_is_valid_should_pass_with_valid_3dtiles(self):
        self.handler.is_valid(files={"base_file": self.valid_tileset}, user=self.user)

    def test_is_valid_should_raise_exception_if_no_basefile_is_supplied(self):
        data = {"base_file": ""}
        with self.assertRaises(Invalid3DTilesException) as _exc:
            self.handler.is_valid(files=data, user=self.user)

        self.assertIsNotNone(_exc)
        self.assertTrue("base file is not provided" in str(_exc.exception.detail))

    def test_extract_params_from_data(self):
        actual, _data = self.handler.extract_params_from_data(
            _data={"defaults": '{"title":"title_of_the_cloned_resource"}'},
            action="copy",
        )

        self.assertEqual(actual, {"title": "title_of_the_cloned_resource"})

    def test_is_valid_should_raise_exception_if_the_3dtiles_is_invalid(self):
        data = {
            "base_file": "/using/double/dot/in/the/name/is/an/error/file.invalid.json"
        }
        with self.assertRaises(Invalid3DTilesException) as _exc:
            self.handler.is_valid(files=data, user=self.user)

        self.assertIsNotNone(_exc)
        self.assertTrue(
            "Please remove the additional dots in the filename"
            in str(_exc.exception.detail)
        )

    def test_is_valid_should_raise_exception_if_the_3dtiles_is_invalid_format(self):
        with self.assertRaises(Invalid3DTilesException) as _exc:
            self.handler.is_valid(
                files={"base_file": self.invalid_tileset}, user=self.user
            )

        self.assertIsNotNone(_exc)
        self.assertTrue(
            "The provided 3DTiles is not valid, some of the mandatory keys are missing. Mandatory keys are: 'asset', 'geometricError', 'root'"
            in str(_exc.exception.detail)
        )

    def test_validate_should_raise_exception_for_invalid_asset_key(self):
        _json = {
            "asset": {"invalid_key": ""},
            "geometricError": 1.0,
            "root": {"boundingVolume": {"box": []}, "geometricError": 0.0},
        }
        _path = "/tmp/tileset.json"
        with open(_path, "w") as _f:
            _f.write(json.dumps(_json))
        with self.assertRaises(Invalid3DTilesException) as _exc:
            self.handler.is_valid(files={"base_file": _path}, user=self.user)

        self.assertIsNotNone(_exc)
        self.assertTrue(
            "The mandatory 'version' for the key 'asset' is missing"
            in str(_exc.exception.detail)
        )
        os.remove(_path)

    def test_validate_should_raise_exception_for_invalid_root_boundingVolume(self):
        _json = {
            "asset": {"version": "1.1"},
            "geometricError": 1.0,
            "root": {"foo": {"box": []}, "geometricError": 0.0},
        }
        _path = "/tmp/tileset.json"
        with open(_path, "w") as _f:
            _f.write(json.dumps(_json))
        with self.assertRaises(Invalid3DTilesException) as _exc:
            self.handler.is_valid(files={"base_file": _path}, user=self.user)

        self.assertIsNotNone(_exc)
        self.assertTrue(
            "The mandatory 'boundingVolume' for the key 'root' is missing"
            in str(_exc.exception.detail)
        )
        os.remove(_path)

    def test_validate_should_raise_exception_for_invalid_root_geometricError(self):
        _json = {
            "asset": {"version": "1.1"},
            "geometricError": 1.0,
            "root": {"boundingVolume": {"box": []}, "foo": 0.0},
        }
        _path = "/tmp/tileset.json"
        with open(_path, "w") as _f:
            _f.write(json.dumps(_json))
        with self.assertRaises(Invalid3DTilesException) as _exc:
            self.handler.is_valid(files={"base_file": _path}, user=self.user)

        self.assertIsNotNone(_exc)
        self.assertTrue(
            "The mandatory 'geometricError' for the key 'root' is missing"
            in str(_exc.exception.detail)
        )
        os.remove(_path)

    def test_get_ogr2ogr_driver_should_return_the_expected_driver(self):
        expected = ogr.GetDriverByName("3dtiles")
        actual = self.handler.get_ogr2ogr_driver()
        self.assertEqual(type(expected), type(actual))

    def test_can_handle_should_return_true_for_3dtiles(self):
        actual = self.handler.can_handle({"base_file": self.valid_tileset})
        self.assertTrue(actual)

    def test_can_handle_should_return_false_for_other_files(self):
        actual = self.handler.can_handle({"base_file": "random.gpkg"})
        self.assertFalse(actual)

    def test_can_handle_should_return_false_if_no_basefile(self):
        actual = self.handler.can_handle({"base_file": ""})
        self.assertFalse(actual)

    def test_supported_file_extension_config(self):
        """
        should return the expected value
        """
        expected = {
            "id": "3dtiles",
            "label": "3D Tiles",
            "format": "vector",
            "ext": ["json"],
            "optional": ["xml", "sld"],
        }
        actual = self.handler.supported_file_extension_config
        self.assertDictEqual(actual, expected)

    def test_generate_resource_payload(self):
        exec_id = orchestrator.create_execution_request(
            user=self.owner,
            func_name="funct1",
            step="step",
            input_params={"files": self.valid_files, "skip_existing_layer": True},
        )
        _exec_obj = orchestrator.get_execution_object(exec_id)
        expected = dict(
            resource_type="dataset",
            subtype="3dtiles",
            dirty_state=True,
            title="Layer name",
            owner=self.owner,
            asset="asset",
            link_type="uploaded",
            extension="3dtiles",
        )

        actual = self.handler.generate_resource_payload(
            "Layer name", "alternate", "asset", _exec_obj, None
        )
        self.assertDictEqual(actual, expected)

    def test_create_geonode_resource_validate_bbox_with_region(self):
        shutil.copy(self.valid_tileset_with_region, "/tmp/tileset.json")

        exec_id = orchestrator.create_execution_request(
            user=self.owner,
            func_name="funct1",
            step="step",
            input_params={
                "files": {"base_file": "/tmp/tileset.json"},
                "skip_existing_layer": True,
            },
        )

        asset = self.asset_handler.create(
            title="Original",
            owner=self.owner,
            description=None,
            type=str(self.handler),
            files=["/tmp/tileset.json"],
            clone_files=False,
        )

        resource = self.handler.create_geonode_resource(
            "layername",
            "layeralternate",
            execution_id=exec_id,
            resource_type="ResourceBase",
            asset=asset,
        )

        # validate bbox
        default_bbox = [-180.0, 180.0, -90.0, 90.0, "EPSG:4326"]
        self.assertFalse(resource.bbox == default_bbox)
        expected = [
            -75.6144410959485,
            -75.60974751970046,
            40.040721313841274,
            40.04433990901052,
            "EPSG:4326",
        ]
        self.assertTrue(resource.bbox == expected)
