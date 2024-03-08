import logging
from geonode.resource.enumerator import ExecutionRequestAction as exa
from importer.handlers.base import BaseHandler
from importer.handlers.xml.serializer import MetadataFileSerializer
from importer.utils import ImporterRequestAction as ira
from importer.orchestrator import orchestrator
from django.shortcuts import get_object_or_404
from geonode.layers.models import Dataset

logger = logging.getLogger(__name__)


class MetadataFileHandler(BaseHandler):
    """
    Handler to import metadata files into GeoNode data db
    It must provide the task_lists required to comple the upload
    """

    ACTIONS = {
        exa.IMPORT.value: ("start_import", "importer.import_resource"),
        ira.ROLLBACK.value: (),
    }

    @staticmethod
    def has_serializer(data) -> bool:
        _base = data.get("base_file")
        if not _base:
            return False
        if (
            _base.endswith("xml") or _base.endswith("sld")
            if isinstance(_base, str)
            else _base.name.endswith("xml") or _base.name.endswith("sld")
        ):
            return MetadataFileSerializer
        return False

    @property
    def supported_file_extension_config(self):
        return None

    @staticmethod
    def extract_params_from_data(_data, action=None):
        """
        Remove from the _data the params that needs to save into the executionRequest object
        all the other are returned
        """
        return {
            "dataset_title": _data.pop("dataset_title", None),
            "skip_existing_layers": _data.pop("skip_existing_layers", "False"),
            "overwrite_existing_layer": _data.pop("overwrite_existing_layer", "False"),
            "store_spatial_file": _data.pop("store_spatial_files", "True"),
        }, _data

    @staticmethod
    def perform_last_step(execution_id):
        pass

    def import_resource(self, files: dict, execution_id: str, **kwargs):
        _exec = orchestrator.get_execution_object(execution_id)
        # getting the dataset
        alternate = _exec.input_params.get("dataset_title")
        dataset = get_object_or_404(Dataset, alternate=alternate)

        # retrieving the handler used for the dataset
        original_handler = orchestrator.load_handler(
            dataset.resourcehandlerinfo_set.first().handler_module_path
        )()

        self.handle_metadata_resource(_exec, dataset, original_handler)

        dataset.refresh_from_db()

        orchestrator.evaluate_execution_progress(
            execution_id, handler_module_path=str(self)
        )
        return dataset

    def handle_metadata_resource(self, _exec, dataset, original_handler):
        raise NotImplementedError
