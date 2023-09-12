import logging
from geonode.resource.enumerator import ExecutionRequestAction as exa
from importer.handlers.base import BaseHandler
from importer.handlers.metadata.serializer import MetadataFileSerializer
from importer.utils import ImporterRequestAction as ira

logger = logging.getLogger(__name__)


class MetadataFileHandler(BaseHandler):
    """
    Handler to import KML files into GeoNode data db
    It must provide the task_lists required to comple the upload
    """

    ACTIONS = {
        exa.IMPORT.value: (
            "start_import",
            "importer.import_resource"
        ),
        ira.ROLLBACK.value: ()
    }

    @staticmethod
    def has_serializer(_data) -> bool:
        return MetadataFileSerializer

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
        pass

