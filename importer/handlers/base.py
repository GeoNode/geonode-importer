from abc import ABC
import logging

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
    TASKS_LIST = []

    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        # instantiate a new type corresponding to the type of class being defined
        # this is currently RegisterBase but in child classes will be the child class
        new_cls = type.__new__(cls, name, bases, attrs)

        if new_cls.__name__.lower() in cls.REGISTRY:
            _log =  f"The class with name {new_cls.__name__.lower()} is already present in the registry, Please take another name"
            logger.error(_log)
            raise Exception(_log)
        elif not attrs.get("handler"):
            raise Exception("If must define the handler")

        cls.REGISTRY[new_cls.__name__.lower()] = attrs.get("handler")
        return new_cls

    def step_list(self):
        '''
        return the step list for he handler
        '''
        return self.TASKS_LIST

    def is_valid(self, files, user):
        """
        Define basic validation steps
        """
        return NotImplementedError
    
    def extract_resource_name_and_crs(self, files, layer_name, alternate):
        '''
        Function to extract the layer name and the CRS from needed in the 
        publishing phase
        [
            {'name': 'alternate or layer_name', 'crs': 'EPSG:25832'}
        ]
        '''
        return NotImplementedError
    
    def create_error_log(self, exc, task_name, *args):
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

