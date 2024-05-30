import json
import logging
import os
from pathlib import Path
from geonode.assets.utils import get_default_asset
from geonode.layers.models import Dataset
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.utils import UploadLimitValidator
from importer.orchestrator import orchestrator
from importer.celery_tasks import import_orchestrator
from importer.handlers.common.vector import BaseVectorFileHandler
from importer.handlers.utils import create_alternate, should_be_imported
from importer.publisher import DataPublisher
from importer.utils import ImporterRequestAction as ira
from geonode.base.models import ResourceBase
from importer.handlers.tiles3d.exceptions import Invalid3DTilesException

logger = logging.getLogger(__name__)


class Tiles3DFileHandler(BaseVectorFileHandler):
    """
    Handler to import 3Dtiles files into GeoNode data db
    It must provide the task_lists required to comple the upload
    """

    ACTIONS = {
        exa.IMPORT.value: (
            "start_import",
            "importer.import_resource",
            "importer.create_geonode_resource",
        ),
        exa.COPY.value: (
            "start_copy",
            "importer.copy_geonode_resource",
        ),
        ira.ROLLBACK.value: (
            "start_rollback",
            "importer.rollback",
        ),
    }

    @property
    def supported_file_extension_config(self):
        return {
            "id": "3dtiles",
            "label": "3D Tiles",
            "format": "vector",
            "ext": ["json"],
            "optional": ["xml", "sld"],
        }

    @staticmethod
    def can_handle(_data) -> bool:
        """
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        """
        base = _data.get("base_file")
        if not base:
            return False
        ext = base.split(".")[-1] if isinstance(base, str) else base.name.split(".")[-1]
        input_filename = os.path.basename(base if isinstance(base, str) else base.name)
        if ext in ["json"] and "tileset.json" in input_filename:
            return True
        return False

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps:
        """
        # calling base validation checks
        BaseVectorFileHandler.is_valid(files, user)
        # getting the upload limit validation
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()

        _file = files.get("base_file")
        if not _file:
            raise Invalid3DTilesException("base file is not provided")

        filename = os.path.basename(_file)

        if len(filename.split(".")) > 2:
            # means that there is a dot other than the one needed for the extension
            # if we keep it ogr2ogr raise an error, better to remove it
            raise Invalid3DTilesException(
                "Please remove the additional dots in the filename"
            )

        try:
            with open(_file, "r") as _readed_file:
                json.loads(_readed_file.read())
        except Exception:
            raise Invalid3DTilesException("The provided GeoJson is not valid")

        return True

    @staticmethod
    def extract_params_from_data(_data, action=None):
        """
        Remove from the _data the params that needs to save into the executionRequest object
        all the other are returned
        """
        if action == exa.COPY.value:
            title = json.loads(_data.get("defaults"))
            return {"title": title.pop("title")}, _data

        return {
            "skip_existing_layers": _data.pop("skip_existing_layers", "False"),
            "overwrite_existing_layer": _data.pop("overwrite_existing_layer", "False"),
            "store_spatial_file": _data.pop("store_spatial_files", "True"),
            "source": _data.pop("source", "upload"),
            "original_zip_name": _data.pop("original_zip_name", None),
        }, _data

    def import_resource(self, files: dict, execution_id: str, **kwargs) -> str:
        logger.info("Total number of layers available: 1")

        _exec = self._get_execution_request_object(execution_id)

        _input = {**_exec.input_params, **{"total_layers": 1}}

        orchestrator.update_execution_request_status(
            execution_id=str(execution_id), input_params=_input
        )
        filename = (
            _exec.input_params.get("original_zip_name")
            or Path(files.get("base_file")).ste
        )
        # start looping on the layers available
        layer_name = self.fixup_name(filename)
        should_be_overwritten = _exec.input_params.get("overwrite_existing_layer")
        # should_be_imported check if the user+layername already exists or not
        if should_be_imported(
            layer_name,
            _exec.user,
            skip_existing_layer=_exec.input_params.get("skip_existing_layer"),
            overwrite_existing_layer=should_be_overwritten,
        ):

            user_datasets = ResourceBase.objects.filter(
                owner=_exec.user, alternate=layer_name
            )

            dataset_exists = user_datasets.exists()

            if dataset_exists and should_be_overwritten:
                layer_name, alternate = (
                    layer_name,
                    user_datasets.first().alternate.split(":")[-1],
                )
            elif not dataset_exists:
                alternate = layer_name
            else:
                alternate = create_alternate(layer_name, execution_id)

        import_orchestrator.apply_async(
            (
                files,
                execution_id,
                str(self),
                "importer.import_resource",
                layer_name,
                alternate,
                exa.IMPORT.value,
            )
        )
        return layer_name, alternate, execution_id

    def create_geonode_resource(
        self,
        layer_name: str,
        alternate: str,
        execution_id: str,
        resource_type: Dataset = ...,
        asset=None,
    ):
        resource = super().create_geonode_resource(
            layer_name, alternate, execution_id, ResourceBase, asset
        )
        # we want just the tileset.json as location of the asset
        asset = get_default_asset(resource)
        asset.location = [path for path in asset.location if "tileset.json" in path]
        asset.save()
        return resource

    def generate_resource_payload(self, layer_name, alternate, asset, _exec, workspace):
        return dict(
            subtype="3dtiles",
            dirty_state=True,
            title=layer_name,
            owner=_exec.user,
            asset=asset,
        )
