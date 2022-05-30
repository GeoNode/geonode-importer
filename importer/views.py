from celery import Celery
from django.contrib.auth import get_user_model
from geonode.tasks.tasks import FaultTolerantTask
from importer.orchestrator import ImportOrchestrator


app = Celery('importer')



@app.task(
    bind=True,
    base=FaultTolerantTask,
    name='importer.start_dataset_import',
    queue='geonode.dataset_importer',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 3},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False
)
def start_dataset_import(self, files, store_spatial_files, user, execution_id=None):
    importer = ImportOrchestrator()

    handler = importer.get_file_handler('gpkg')

    if execution_id is None:
        execution_id = importer.create_execution_request(
            user=get_user_model().objects.get(username=user),
            func_name="start_dataset_import",
            input_params={
                "step": next(iter(handler.TASKS_LIST)),
                "files": files,
                "store_spatial_files": store_spatial_files
            }
        )

    importer.perform_next_import_step(resource_type="gpkg", execution_id=execution_id)




@app.task(
    bind=True,
    base=FaultTolerantTask,
    name='importer.import_resource',
    queue='geonode.dataset_importer',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 3},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False
)
def import_resource(self, execution_id):
    pass