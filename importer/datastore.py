from importer.orchestrator import SUPPORTED_TYPES


class DataStoreManager:
    '''
    Utility object to invoke the right handler used to save the
    resource in the datastore db
    '''
    def __init__(self, files: list, resource_type: str) -> None:
        self.files = files
        self.handler = SUPPORTED_TYPES.get(resource_type)

    def input_is_valid(self):
        """
        Perform basic validation steps
        """
        return self.handler.is_valid(self.files)

    def start_import(self, execution_id):
        '''
        call the resource handler object to perform the import phase
        '''
        return self.handler.import_resource(self.files, execution_id)