import logging
import os
from django.conf import settings
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.utils import UploadLimitValidator
from importer.handlers.common.vector import BaseVectorFileHandler
from osgeo import ogr

from importer.handlers.geojson.exceptions import InvalidGeoJsonException

logger = logging.getLogger(__name__)


class GeoJsonFileHandler(BaseVectorFileHandler):
    '''
    Handler to import GeoJson files into GeoNode data db
    It must provide the task_lists required to comple the upload
    '''

    ACTIONS = {
        exa.IMPORT.value: (
            "start_import",
            "importer.import_resource",
            "importer.publish_resource",
            "importer.create_geonode_resource"
        ),
        exa.COPY.value: (
            "start_copy",
            "importer.copy_geonode_resource",
            "importer.copy_dynamic_model",
            "importer.copy_geonode_data_table",
            "importer.publish_resource"
        ),
    }
   

    @staticmethod
    def can_handle(_data) -> bool:
        '''
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        '''
        base = _data.get("base_file")
        if not base:
            return False
        ext = base.split('.')[-1] if isinstance(base, str) else base.name.split('.')[-1]
        return ext in ['json', 'geojson']

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps:
        """
        # getting the upload limit validation
        upload_validator = UploadLimitValidator(user)
        upload_validator.validate_parallelism_limit_per_user()
        
        _file = files.get('base_file')
        if not _file:
            raise InvalidGeoJsonException("base file is not provided")

        filename = os.path.basename(_file)

        if len(filename.split('.')) > 2:
            # means that there is a dot other than the one needed for the extension
            # if we keep it ogr2ogr raise an error, better to remove it
            raise InvalidGeoJsonException("Please remove the additional dots in the filename")

        return True

    def get_ogr2ogr_driver(self):
        return ogr.GetDriverByName("GeoJSON")
    
    @staticmethod
    def create_ogr2ogr_command(files, original_name, override_layer, alternate):
        '''
        Define the ogr2ogr command to be executed.
        This is a default command that is needed to import a vector file
        '''
        
        base_command = BaseVectorFileHandler.create_ogr2ogr_command(files, original_name, override_layer, alternate)
        return f"{base_command } -lco GEOMETRY_NAME={BaseVectorFileHandler().default_geometry_column_name}" 