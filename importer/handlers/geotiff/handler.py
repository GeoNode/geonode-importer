import logging
import os

from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.utils import UploadLimitValidator
from importer.handlers.common.raster import BaseRasterFileHandler
from importer.handlers.geotiff.exceptions import InvalidGeoTiffException

logger = logging.getLogger(__name__)


class GeoTiffFileHandler(BaseRasterFileHandler):
    """
    Handler to import GeoTiff files into GeoNode data db
    It must provide the task_lists required to comple the upload
    """

    ACTIONS = {
        exa.IMPORT.value: (
            "start_import",
            "importer.import_resource",
            "importer.publish_resource",
            "importer.create_geonode_resource",
        ),
        exa.COPY.value: (
            "start_copy",
            "importer.copy_raster_file",
            "importer.publish_resource",
            "importer.copy_geonode_resource",
        ),
    }

    @property
    def supported_file_extension_config(self):
        return {
            "id": 'tiff',
            "label": 'GeoTIFF',
            "format": 'raster',
            "ext": ['tiff', 'tif'],
            "mimeType": ['image/tiff'],
            "optional": ['xml', 'sld']
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
        return ext in ["tiff", "geotiff", "tif"]

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps:
        """
        # calling base validation checks
        BaseRasterFileHandler.is_valid(files, user)
        # getting the upload limit validation
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()

        _file = files.get("base_file")
        if not _file:
            raise InvalidGeoTiffException("base file is not provided")

        filename = os.path.basename(_file)

        if len(filename.split(".")) > 2:
            # means that there is a dot other than the one needed for the extension
            # if we keep it ogr2ogr raise an error, better to remove it
            raise InvalidGeoTiffException(
                "Please remove the additional dots in the filename"
            )
        return True
