import logging
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.utils import UploadLimitValidator
from importer.handlers.common.vector import BaseVectorFileHandler


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

        return True
