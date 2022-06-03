from celery import Celery
from celery.worker.request import Request


class ImporterCeleryTask(Request):
    abstract = True

    def on_failure(self, exc_info, send_failed_event=True, return_ok=False):
        # exc (Exception) - The exception raised by the task.
        # args (Tuple) - Original arguments for the task that failed.
        # kwargs (Dict) - Original keyword arguments for the
        print(exc_info)
        exc_info


app = Celery("importer")
