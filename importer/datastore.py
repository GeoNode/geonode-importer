from importer.orchestrator import SUPPORTED_TYPES


class DataStoreManager:
    def __init__(self, files: list, resource_type: str) -> None:
        self.files = files
        self.handler = SUPPORTED_TYPES.get(resource_type)

    def input_is_valid(self):
        """
        Perform basic validation steps
        """
        return self.handler.is_valid(self.files)

    def start_import(self, execution_id):
        return self.handler.start_import(self.files)