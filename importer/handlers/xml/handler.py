import logging

from django.shortcuts import get_object_or_404
from geonode.layers.models import Dataset
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.resource.manager import resource_manager
from importer.handlers.common.metadata import MetadataFileHandler
from importer.handlers.xml.exceptions import InvalidXmlException
from importer.orchestrator import orchestrator
from owslib.etree import etree as dlxml

logger = logging.getLogger(__name__)


class XMLFileHandler(MetadataFileHandler):
    """
    Handler to import KML files into GeoNode data db
    It must provide the task_lists required to comple the upload
    """

    @staticmethod
    def can_handle(_data) -> bool:
        """
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        """
        base = _data.get("base_file")
        if not base:
            return False
        return (
            base.endswith(".xml")
            if isinstance(base, str)
            else base.name.endswith(".xml")
        )

    @staticmethod
    def is_valid(files, user=None):
        """
        Define basic validation steps
        """
        # calling base validation checks
 
        try:
            with open(files.get("base_file")) as _xml:
                dlxml.fromstring(_xml.read().encode())
        except Exception as err:
            raise InvalidXmlException(f"Uploaded document is not XML or is invalid: {str(err)}")
        return True

    def import_resource(self, files: dict, execution_id: str, **kwargs):
        _exec = orchestrator.get_execution_object(execution_id)
        # getting the dataset
        alternate = _exec.input_params.get("dataset_title")
        dataset = get_object_or_404(Dataset, alternate=alternate)

        # retrieving the handler used for the dataset
        original_handler = orchestrator.load_handler(
            dataset.resourcehandlerinfo_set\
                .first()\
                .handler_module_path
        )()

        if original_handler.can_handle_xml_file:
            original_handler.handle_xml_file(dataset, _exec)
        else:
            _path = _exec.input_params.get("files", {}).get("xml_file", _exec.input_params.get("base_file", {}))
            resource_manager.update(
                None,
                instance=dataset,
                xml_file=_path,
                metadata_uploaded=True if _path else False,
                vals={"dirty_state": True},
            )
        dataset.refresh_from_db()
        
        orchestrator.evaluate_execution_progress(execution_id, handler_module_path=str(self))
        return

