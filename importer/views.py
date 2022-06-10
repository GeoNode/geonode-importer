import logging

from celery import Task
from django.contrib.auth import get_user_model
from django.utils import timezone
from geonode.layers.models import Dataset
from geonode.resource.manager import resource_manager
from geonode.resource.models import ExecutionRequest
from geonode.storage.manager import storage_manager

from importer.api.exception import (StartImportException, InvalidInputFileException,
                                    PublishResourceException, ResourceCreationException)
from importer.celery_app import importer_app
from importer.datastore import DataStoreManager
from importer.orchestrator import ImportOrchestrator
from importer.publisher import DataPublisher
import pathlib


importer = ImportOrchestrator()
logger = logging.getLogger(__name__)


class ErrorBaseClassForTask(Task):

    max_retries = 3

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # exc (Exception) - The exception raised by the task.
        # args (Tuple) - Original arguments for the task that failed.
        # kwargs (Dict) - Original keyword arguments for the task that failed.ù
        logger.error(f"Task FAILED with ID: {args[1]}, reason: {exc}")
        importer.set_as_failed(
            execution_id=args[1], reason=str(exc.detail if hasattr(exc, "detail") else exc)
        )
        # To keep the upload folder under control
        # if a workflow fail, we delete the uploaded files
        exc_obj = importer.get_execution_object(args[1])
        _files_list = exc_obj.input_params.get("files", [])
        if _files_list:
            for _file in _files_list.values():
                logging.info(f"deleting file: {_file}")
                storage_manager.delete(_file)


@importer_app.task(
    base=ErrorBaseClassForTask,
    name="importer.import_orchestrator",
    queue="importer.import_orchestrator",
    max_retries=1
)
def import_orchestrator(
    files, store_spatial_files=True, user=None, execution_id=None
):
    try:

        file_ext = pathlib.Path(files.get("base_file")).suffix[1:]
        handler = importer.get_file_handler(file_ext)

        if execution_id is None:
            logger.info("Execution ID is None, creating....")
            execution_id = importer.create_execution_request(
                user=get_user_model().objects.get(username=user),
                func_name=next(iter(handler.TASKS_LIST)),
                step=next(iter(handler.TASKS_LIST)),
                input_params={"files": files, "store_spatial_files": store_spatial_files},
            )

        importer.perform_next_import_step(resource_type="gpkg", execution_id=execution_id)
    except Exception as e:
        raise StartImportException(e.args[0])


@importer_app.task(
    base=ErrorBaseClassForTask,    
    name="importer.import_resource",
    queue="importer.import_resource",
    max_retries=1
)
def import_resource(resource_type, execution_id):
    '''
    Task to import the resources in geoserver
    after updating the execution status will perform a small data_validation
    implemented inside the filetype handler.
    If is the resource is valid, the start_import method of the handler
    is called to proceed with the import
    '''    
    # Updating status to running
    try:
        importer.update_execution_request_status(
            execution_id=execution_id,
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="import_resource",
            step="importer.import_resource",
        )
        _exec = importer.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")
        _store_spatial_files = _exec.input_params.get("store_spatial_files")
        _user = _exec.user

        _datastore = DataStoreManager(_files, resource_type)

        # starting file validation
        if not _datastore.input_is_valid():
            raise Exception("dataset is invalid")

        # do something
        _datastore.start_import(execution_id)

        # at the end recall the import_orchestrator for the next step
        import_orchestrator.apply_async(
            (_files, _store_spatial_files, _user.username, execution_id)
        )
    except Exception as e:
        raise InvalidInputFileException(detail=e.args[0])


@importer_app.task(
    base=ErrorBaseClassForTask,    
    name="importer.publish_resource",
    queue="importer.publish_resource",
    max_retries=1
)
def publish_resource(resource_type, execution_id):
    '''
    Task to publish the resources on geoserver
    It will take the layers name from the source file
    The layers name rappresent the table names that were saved
    in geoserver in the previous step.
    At the end of the execution the main import_orchestrator is called
    to proceed to the next step if available
    '''
    # Updating status to running

    try:
        importer.update_execution_request_status(
            execution_id=execution_id,
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="publish_resource",
            step="importer.publish_resource",
        )
        _exec = importer.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")
        _store_spatial_files = _exec.input_params.get("store_spatial_files")
        _user = _exec.user

        _publisher = DataPublisher()
        _metadata = _publisher._extract_resource_name_from_file(_files, resource_type)
        _, workspace, store = _publisher.publish_resources(_metadata)

        importer.update_execution_request_status(
            execution_id=execution_id,
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            input_params={**_exec.input_params, **{"workspace": workspace, "store": store}}
        )

        # at the end recall the import_orchestrator for the next step
        import_orchestrator.apply_async(
            (_files, _store_spatial_files, _user.username, execution_id)
        )

    except Exception as e:
        raise PublishResourceException(detail=e.args[0])


@importer_app.task(
    base=ErrorBaseClassForTask,
    name="importer.create_gn_resource",
    queue="importer.create_gn_resource",
    max_retries=1

)
def create_gn_resource(resource_type, execution_id):
    '''
    Create the GeoNode resource and the relatives information associated
    '''
    # Updating status to running
    try:
        importer.update_execution_request_status(
            execution_id=execution_id,
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="create_gn_resource",
            step="importer.create_gn_resource",
        )
        _exec = importer.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")
        _store_spatial_files = _exec.input_params.get("store_spatial_files")
        metadata_uploaded = _files.get("xml_file", "") or False
        sld_uploaded = _files.get("sld_uploaded", "") or False
        _user = _exec.user
        
        _publisher = DataPublisher()
        resources = _publisher._extract_resource_name_from_file(_files, resource_type)
        for resource in resources:
            # update the last_updated value to evaluate that the task is still running
            importer.update_execution_request_status(
                status=ExecutionRequest.STATUS_RUNNING,
                execution_id=execution_id,
                last_updated=timezone.now(),
                log=f"creating dataset for resource: {resource.get('name')}"
            )        
            saved_dataset = resource_manager.create(
                None,
                resource_type=Dataset,
                defaults=dict(
                    name=resource.get("name"),
                    workspace=_exec.input_params.get("workspace", "geonode"),
                    store=_exec.input_params.get("store", "geonode_data"),
                    subtype='vector',
                    alternate=resource.get("name"),
                    title=resource.get("name"),
                    owner=_user,
                    files=_files,
                    srid=resource.get("crs"),
                )
            )
            if metadata_uploaded:
                resource_manager.update(None,
                    instance=saved_dataset,
                    xml_file=_files.get("xml_file", ""),
                    metadata_uploaded=metadata_uploaded
                )
            if sld_uploaded:
                resource_manager.exec(
                    'set_style',
                    None,
                    instance=saved_dataset,
                    sld_uploaded=sld_uploaded,
                    sld_file=_files.get("sld_file", "")
                )
            resource_manager.set_thumbnail(None, instance=saved_dataset)

        if not _store_spatial_files:
            storage_manager.delete(_files.values())
        # at the end recall the import_orchestrator for the next step
        import_orchestrator.apply_async(
            (_files, _store_spatial_files, _user.username, execution_id)
        )
    except Exception as e:
        raise ResourceCreationException(detail=e.args[0])
