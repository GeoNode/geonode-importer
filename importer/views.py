from celery import Celery
from django.contrib.auth import get_user_model
from django.utils import timezone
from geonode.resource.models import ExecutionRequest
from geonode.tasks.tasks import FaultTolerantTask

from importer.datastore import DataStoreManager
from importer.orchestrator import ImportOrchestrator

app = Celery('importer')
importer = ImportOrchestrator()


@app.task(
    bind=True,
    base=FaultTolerantTask,
    name='importer.import_orchestrator',
    queue='importer.import_orchestrator',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 3},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False
)
def import_orchestrator(self, files, store_spatial_files=True, user=None, execution_id=None):
    # TODO: get filetybe by the files
    handler = importer.get_file_handler('gpkg')

    if execution_id is None:
        execution_id = importer.create_execution_request(
            user=get_user_model().objects.get(username=user),
            func_name=next(iter(handler.TASKS_LIST)),
            step=next(iter(handler.TASKS_LIST)),
            input_params={
                "files": files,
                "store_spatial_files": store_spatial_files
            }
        )

    importer.perform_next_import_step(resource_type="gpkg", execution_id=execution_id)


@app.task(
    bind=True,
    base=FaultTolerantTask,
    name='importer.import_resource',
    queue='importer.import_resource',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 3},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False
)
def import_resource(self, resource_type, execution_id):
    # Updating status to running
    importer.update_execution_request_status(
        execution_id=execution_id,
        status=ExecutionRequest.STATUS_RUNNING,
        last_updated=timezone.now(),
        func_name="import_resource",
        step="importer.import_resource"
    )
    _exec = importer._get_execution_object(execution_id)

    _files = _exec.input_params.get("files")
    _store_spatial_files = _exec.input_params.get("files")
    _user = _exec.user
    handler = DataStoreManager(_files, resource_type)

    handler.is_valid()
    # starting file validation

    # do something


    #at the end recall the import_orchestrator for the next step
    import_orchestrator.apply_async(
                    (_files, _store_spatial_files, _user.username, execution_id)
                )
    pass
