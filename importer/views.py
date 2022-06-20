import logging
import pathlib
from typing import Optional

from django.contrib.auth import get_user_model
from django.utils import timezone
from geonode.layers.models import Dataset
from geonode.resource.manager import resource_manager
from geonode.resource.models import ExecutionRequest
from geonode.storage.manager import storage_manager

from importer.api.exception import (InvalidInputFileException,
                                    PublishResourceException,
                                    ResourceCreationException,
                                    StartImportException)
from importer.celery_app import importer_app
from importer.celery_tasks import ErrorBaseTaskClass
from importer.datastore import DataStoreManager
from importer.orchestrator import orchestrator
from importer.publisher import DataPublisher

logger = logging.getLogger(__name__)


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,
    name="importer.import_orchestrator",
    queue="importer.import_orchestrator",
    max_retries=1
)
def import_orchestrator(
    self, files, store_spatial_files=True, user=None, execution_id=None, step='start_import', layer_name=None, alternate=None
):
    try:
        file_ext = pathlib.Path(files.get("base_file")).suffix[1:]
        handler = orchestrator.get_file_handler(file_ext)

        if execution_id is None:
            logger.info("Execution ID is None, creating....")
            execution_id = orchestrator.create_execution_request(
                user=get_user_model().objects.get(username=user),
                func_name=next(iter(handler.TASKS_LIST)),
                step=next(iter(handler.TASKS_LIST)),
                input_params={
                    "files": files,
                    "store_spatial_files": store_spatial_files
                },
            )

        orchestrator.perform_next_import_step(
            resource_type="gpkg",
            execution_id=execution_id,
            step=step,
            layer_name=layer_name,
            alternate=alternate
        )
    except Exception as e:
        raise StartImportException(e.args[0])


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,    
    name="importer.import_resource",
    queue="importer.import_resource",
    max_retries=2
)
def import_resource(self, execution_id, /, resource_type):
    '''
    Task to import the resources in geoserver
    after updating the execution status will perform a small data_validation
    implemented inside the filetype handler.
    If is the resource is valid, the start_import method of the handler
    is called to proceed with the import
    '''    
    # Updating status to running
    try:
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="import_resource",
            step="importer.import_resource",
        )
        _exec = orchestrator.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")

        _datastore = DataStoreManager(_files, resource_type)

        # starting file validation
        if not _datastore.input_is_valid():
            raise Exception("dataset is invalid")

        _datastore.start_import(execution_id)

        return

    except Exception as e:
        raise InvalidInputFileException(detail=e.args[0])


@importer_app.task(
    base=ErrorBaseTaskClass,    
    name="importer.publish_resource",
    queue="importer.publish_resource",
    max_retries=1,
    rate_limit=3
)
def publish_resource(
    execution_id: str,
    /,
    resource_type: str,
    step_name: str,
    layer_name: Optional[str] = None,
    alternate: Optional[str] = None
):
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
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="publish_resource",
            step="importer.publish_resource",
        )
        _exec = orchestrator.get_execution_object(execution_id)
        _files = _exec.input_params.get("files")
        _store_spatial_files = _exec.input_params.get("store_spatial_files")
        _overwrite = _exec.input_params.get("override_existing_layer")
        _user = _exec.user
        if _overwrite:
            # for now we dont heve the overwrite option in GS, skipping will we talk with the GS team
            return
        _publisher = DataPublisher()
        _metadata = _publisher.extract_resource_name_and_crs(_files, resource_type, layer_name, alternate)
        if _metadata and _metadata[0].get("crs"):
            # we should not publish resource without a crs
            _, workspace, store = _publisher.publish_resources(_metadata)

            orchestrator.update_execution_request_status(
                execution_id=execution_id,
                status=ExecutionRequest.STATUS_RUNNING,
                last_updated=timezone.now(),
                input_params={**_exec.input_params, **{"workspace": workspace, "store": store}}
            )

        # at the end recall the import_orchestrator for the next step
        import_orchestrator.apply_async(
            (_files, _store_spatial_files, _user.username, execution_id, step_name, layer_name, alternate)
        )

    except Exception as e:
        raise PublishResourceException(detail=e.args[0])


@importer_app.task(
    base=ErrorBaseTaskClass,
    name="importer.create_gn_resource",
    queue="importer.create_gn_resource",
    max_retries=1,
    rate_limit=3
)
def create_gn_resource(
    execution_id: str,
    /,
    resource_type: str,
    step_name: str,
    layer_name: Optional[str] = None,
    alternate: Optional[str] = None
):
    '''
    Create the GeoNode resource and the relatives information associated
    '''
    # Updating status to running
    try:
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="create_gn_resource",
            step="importer.create_gn_resource",
        )
        _exec = orchestrator.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")
        _store_spatial_files = _exec.input_params.get("store_spatial_files")
        metadata_uploaded = _files.get("xml_file", "") or False
        sld_uploaded = _files.get("sld_uploaded", "") or False
        _user = _exec.user

        orchestrator.update_execution_request_status(
            status=ExecutionRequest.STATUS_RUNNING,
            execution_id=execution_id,
            last_updated=timezone.now(),
            log=f"Creating GN dataset for resource: {alternate}:"
        )        
        saved_dataset = resource_manager.create(
            None,
            resource_type=Dataset,
            defaults=dict(
                name=alternate,
                workspace=_exec.input_params.get("workspace", "geonode"),
                store=_exec.input_params.get("store", "geonode_data"),
                subtype='vector',
                alternate=alternate,
                title=layer_name,
                owner=_user,
                files=_files,
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

        # at the end recall the import_orchestrator for the next step
        import_orchestrator.apply_async(
            (_files, _store_spatial_files, _user.username, execution_id, step_name, layer_name, alternate)
        )

    except Exception as e:
        raise ResourceCreationException(detail=e.args[0])
