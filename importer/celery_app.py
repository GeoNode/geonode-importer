from celery import Celery, Task
from celery.worker.request import Request


importer_app = Celery("importer")
