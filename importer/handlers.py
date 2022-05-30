from abc import ABC

class AbstractHandler(ABC):
    TASKS_LIST = []

    def step_list(self):
        return self.TASKS_LIST


class GPKGStepHandler(AbstractHandler):
    TASKS_LIST = (
        "start_import",
        "importer.import_resource",
        "importer.publish_resource",
        "importer.create_resource",
    )
