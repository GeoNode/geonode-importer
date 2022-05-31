from abc import ABC
import os


class AbstractHandler(ABC):
    TASKS_LIST = []

    def step_list(self):
        return self.TASKS_LIST

    def is_valid(self):
        """
        Define basic validation steps
        """
        return NotImplementedError


class GPKGFileHandler(AbstractHandler):
    TASKS_LIST = (
        "start_import",
        "importer.import_resource",
        "importer.publish_resource",
        "importer.create_gn_resource",
    )

    def is_valid(self, files):
        return all([os.path.exists(x) for x in files])
