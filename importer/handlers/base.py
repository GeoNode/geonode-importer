from abc import ABC
import logging
from typing import List

logger = logging.getLogger(__name__)

class BaseHandler(ABC):
    '''
    Base abstract handler object
    define the required method needed to define an upload handler
    it must be:
    - provide the tasks list to complete the import
    - validation function
    - method to import the resource
    - create_error_log
    '''

    REGISTRY = []

    def __str__(self):
        return f"{self.__module__}.{self.__class__.__name__}"

    def __repr__(self):
        return self.__str__()

    @classmethod
    def register(cls):
        BaseHandler.REGISTRY.append(cls)

    @classmethod
    def get_registry(cls):
        return BaseHandler.REGISTRY

    @staticmethod
    def can_handle(_data) -> bool:
        '''
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        '''
        return False

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps
        """
        return NotImplementedError
    
    @staticmethod
    def extract_params_from_data(_data):
        '''
        Remove from the _data the params that needs to save into the executionRequest object
        all the other are returned
        '''
        return []

    @staticmethod
    def extract_resource_to_publish(files, layer_name, alternate):
        '''
        Function to extract the layer name and the CRS from needed in the 
        publishing phase
        [
            {'name': 'alternate or layer_name', 'crs': 'EPSG:25832'}
        ]
        '''
        return NotImplementedError

    @staticmethod
    def create_error_log(exc, task_name, *args):
        '''
        This function will handle the creation of the log error for each message.
        This is helpful and needed, so each handler can specify the log as needed
        '''
        return NotImplementedError

    def import_resource(self, files: dict, execution_id: str, **kwargs):
        '''
        Define the step to perform the import of the data
        into the datastore db
        '''
        return NotImplementedError
    
    @staticmethod
    def publish_resources(resources: List[str], catalog, store, workspace):
        '''
        Function (if needed) to publish the resource into GeoServer
        '''
        return NotImplementedError
