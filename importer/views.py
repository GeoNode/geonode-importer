from django.contrib.auth import get_user_model
from django.utils import timezone
from geonode.resource.models import ExecutionRequest
from geonode.tasks.tasks import FaultTolerantTask
from importer.api.exception import InvalidInputFileException
from importer.celery_app import app
from importer.datastore import DataStoreManager
from importer.orchestrator import ImportOrchestrator
from importer.publisher import DataPublisher

importer = ImportOrchestrator()


@app.task(
    bind=True,
    base=FaultTolerantTask,
    name="importer.import_orchestrator",
    queue="importer.import_orchestrator",
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False,
)
def import_orchestrator(
    self, files, store_spatial_files=True, user=None, execution_id=None
):
    # TODO: get filetype by the files
    handler = importer.get_file_handler("gpkg")

    if execution_id is None:
        execution_id = importer.create_execution_request(
            user=get_user_model().objects.get(username=user),
            func_name=next(iter(handler.TASKS_LIST)),
            step=next(iter(handler.TASKS_LIST)),
            input_params={"files": files, "store_spatial_files": store_spatial_files},
        )

    importer.perform_next_import_step(resource_type="gpkg", execution_id=execution_id)


@app.task(
    bind=True,
    base=FaultTolerantTask,
    name="importer.import_resource",
    queue="importer.import_resource",
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False,
)
def import_resource(self, resource_type, execution_id):
    # Updating status to running
    importer.update_execution_request_status(
        execution_id=execution_id,
        status=ExecutionRequest.STATUS_RUNNING,
        last_updated=timezone.now(),
        func_name="import_resource",
        step="importer.import_resource",
    )
    _exec = importer.get_execution_object(execution_id)

    _files = _exec.input_params.get("files")
    _store_spatial_files = _exec.input_params.get("files")
    _user = _exec.user

    _datastore = DataStoreManager(_files, resource_type)

    # starting file validation
    if not _datastore.input_is_valid():
        importer.set_as_failed(execution_id=execution_id)
        raise InvalidInputFileException()

    # do something
    _datastore.start_import(execution_id)

    # at the end recall the import_orchestrator for the next step
    import_orchestrator.apply_async(
        (_files, _store_spatial_files, _user.username, execution_id)
    )


@app.task(
    bind=True,
    base=FaultTolerantTask,
    name="importer.publish_resource",
    queue="importer.publish_resource",
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False,
)
def publish_resource(self, _, execution_id):
    # Updating status to running
    importer.update_execution_request_status(
        execution_id=execution_id,
        status=ExecutionRequest.STATUS_RUNNING,
        last_updated=timezone.now(),
        func_name="publish_resource",
        step="importer.publish_resource",
    )
    _exec = importer.get_execution_object(execution_id)

    _files = _exec.input_params.get("files")
    _store_spatial_files = _exec.input_params.get("files")
    _user = _exec.user

    _publisher = DataPublisher()

    resources = _publisher._extract_resource_name_from_file(_files)

    _publisher.publish_resources(resources)

    # at the end recall the import_orchestrator for the next step
    import_orchestrator.apply_async(
        (_files, _store_spatial_files, _user.username, execution_id)
    )
