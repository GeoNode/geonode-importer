from abc import ABC


class AbstractHandler(ABC):
    '''
    Base abstract handler object
    define the required method needed to define an upload handler
    it must be:
    - provide the tasks list to complete the import
    - validation function
    - method to import the resource
    '''
    TASKS_LIST = []

    def step_list(self):
        return self.TASKS_LIST

    def is_valid(self):
        """
        Define basic validation steps
        """
        return NotImplementedError
    
    def create_error_log(self, *args):
        '''
        This function will handle the creation of the log error for each message
        '''
        return NotImplementedError

    def import_resource(self):
        '''
        Define the step to perform the import of the data
        into the datastore db
        '''
        return NotImplementedError

