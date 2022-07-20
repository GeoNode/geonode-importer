import logging
import os
from typing import Optional
from uuid import UUID

from celery import Task
from django.utils import timezone
from django.utils.module_loading import import_string

from importer.api.exception import (InvalidInputFileException,
                                    PublishResourceException,
                                    ResourceCreationException,
                                    StartImportException)
from importer.celery_app import importer_app
from importer.datastore import DataStoreManager
from importer.models import ResourceHandlerInfo
from importer.orchestrator import orchestrator
from importer.publisher import DataPublisher
from importer.settings import (IMPORTER_GLOBAL_RATE_LIMIT,
                               IMPORTER_PUBLISHING_RATE_LIMIT,
                               IMPORTER_RESOURCE_CREATION_RATE_LIMIT)
from importer.utils import error_handler

logger = logging.getLogger(__name__)


class ErrorBaseTaskClass(Task):
    '''
    Basic Error task class. Is common to all the base tasks of the import pahse
    it defines a on_failure method which set the task as "failed" with some extra information
    '''
    max_retries = 3
    track_started=True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # exc (Exception) - The exception raised by the task.
        # args (Tuple) - Original arguments for the task that failed.
        # kwargs (Dict) - Original keyword arguments for the task that failed.
        _uuid = self._get_uuid(args)

        logger.error(f"Task FAILED with ID: {_uuid}, reason: {exc}")

        orchestrator.set_as_failed(
            execution_id=_uuid, reason=str(exc.detail if hasattr(exc, "detail") else exc.args[0])
        )

    def _get_uuid(self, _list):
        for el in _list:
            try:
                UUID(el)
                return el
            except:
                continue


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,
    name="importer.import_orchestrator",
    queue="importer.import_orchestrator",
    max_retries=1,
    rate_limit=IMPORTER_GLOBAL_RATE_LIMIT,
    task_track_started=True
)
def import_orchestrator(
    self, files: dict, execution_id: str, handler=None, step='start_import', layer_name=None, alternate=None
):

    '''
    Base task. Is the task responsible to call the orchestrator and redirect the upload to the next step
    mainly is a wrapper for the Orchestrator object.

            Parameters:
                    files (dict): dictionary with the files needed for the import. it expect that there is always a base_file
                                  example: {"base_file": "/path/to/the/local/file/to/be/importerd.gpkg"}
                    user (UserModel): user that is performing the request
                    execution_id (UUID): unique ID used to keep track of the execution request
                    step (str): last step performed from the tasks
                    layer_name (str): layer name
                    alternate (str): alternate used to naming the layer
            Returns:
                    None
    '''
    try:
       # extract the resource_type of the layer and retrieve the expected handler

        orchestrator.perform_next_import_step(
            execution_id=execution_id,
            step=step,
            layer_name=layer_name,
            alternate=alternate,
            handler_module_path=handler
        )

    except Exception as e:
        raise StartImportException(detail=error_handler(e))


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,    
    name="importer.import_resource",
    queue="importer.import_resource",
    max_retries=2,
    rate_limit=IMPORTER_GLOBAL_RATE_LIMIT,
    ignore_result=False,
    task_track_started=True
)
def import_resource(self, execution_id, /, handler_module_path):  
    '''
    Task to import the resources.
    NOTE: A validation if done before acutally start the import

            Parameters:
                    execution_id (UUID): unique ID used to keep track of the execution request
                    resource_type (str): extension of the resource type that we want to import
                    The resource type is needed to retrieve the right handler for the resource
            Returns:
                    None
    '''
    # Updating status to running
    try:
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            last_updated=timezone.now(),
            func_name="import_resource",
            step="importer.import_resource",
            celery_task_request=self.request
        )
        _exec = orchestrator.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")

        # initiating the data store manager
        _datastore = DataStoreManager(_files, handler_module_path, _exec.user, execution_id)

        # starting file validation
        if not _datastore.input_is_valid():
            raise Exception("dataset is invalid")

        _datastore.start_import(execution_id)

        '''
        The orchestrator to proceed to the next step, should be called by the hander
        since the call to the orchestrator can changed based on the handler
        called. See the GPKG handler gpkg_next_step task
        '''
        return self.name, execution_id

    except Exception as e:
        raise InvalidInputFileException(detail=error_handler(e))


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,    
    name="importer.publish_resource",
    queue="importer.publish_resource",
    max_retries=3,
    rate_limit=IMPORTER_PUBLISHING_RATE_LIMIT,
    ignore_result=False,
    task_track_started=True
)
def publish_resource(
    self,
    execution_id: str,
    /,
    step_name: str,
    layer_name: Optional[str] = None,
    alternate: Optional[str] = None,
    handler_module_path: str = None
):
    '''
    Task to publish a single resource in geoserver.
    NOTE: If the layer should be overwritten, for now we are skipping this feature
        geoserver is not ready yet

            Parameters:
                    execution_id (UUID): unique ID used to keep track of the execution request
                    step_name (str): step name example: importer.publish_resource
                    layer_name (UUID): name of the resource example: layer
                    alternate (UUID): alternate of the resource example: layer_alternate
            Returns:
                    None
    '''
    # Updating status to running
    try:
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            last_updated=timezone.now(),
            func_name="publish_resource",
            step="importer.publish_resource",
            celery_task_request=self.request
        )
        _exec = orchestrator.get_execution_object(execution_id)
        _files = _exec.input_params.get("files")
        _overwrite = _exec.input_params.get("override_existing_layer")
        
        # for now we dont heve the overwrite option in GS, skipping will we talk with the GS team
        if not _overwrite:
            _publisher = DataPublisher(handler_module_path)

            # extracting the crs and the resource name, are needed for publish the resource
            _metadata = _publisher.extract_resource_to_publish(_files, layer_name, alternate)
            if _metadata:
                # we should not publish resource without a crs

                _publisher.publish_resources(_metadata)

                # updating the execution request status
                orchestrator.update_execution_request_status(
                    execution_id=execution_id,
                    last_updated=timezone.now(),
                    celery_task_request=self.request
                )
            else:
                logger.error("Only resources with a CRS provided can be published")
                raise PublishResourceException("Only resources with a CRS provided can be published")

        # at the end recall the import_orchestrator for the next step

        import_orchestrator.apply_async(
            (_files, execution_id, handler_module_path, step_name, layer_name, alternate)
        )
        return self.name, execution_id

    except Exception as e:
        raise PublishResourceException(detail=error_handler(e))


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,
    name="importer.create_geonode_resource",
    queue="importer.create_geonode_resource",
    max_retries=1,
    rate_limit=IMPORTER_RESOURCE_CREATION_RATE_LIMIT,
    ignore_result=False,
    task_track_started=True
)
def create_geonode_resource(
    self,
    execution_id: str,
    /,
    step_name: str,
    layer_name: Optional[str] = None,
    alternate: Optional[str] = None,
    handler_module_path: str = None
):
    '''
    Create the GeoNode resource and the relatives information associated
    NOTE: for gpkg we dont want to handle sld and XML files

            Parameters:
                    execution_id (UUID): unique ID used to keep track of the execution request
                    resource_type (str): extension of the resource type that we want to import
                        The resource type is needed to retrieve the right handler for the resource
                    step_name (str): step name example: importer.publish_resource
                    layer_name (UUID): name of the resource example: layer
                    alternate (UUID): alternate of the resource example: layer_alternate
            Returns:
                    None
    '''
    # Updating status to running
    try:
        orchestrator.update_execution_request_status(
            execution_id=execution_id,
            last_updated=timezone.now(),
            func_name="create_geonode_resource",
            step="importer.create_geonode_resource",
            celery_task_request=self.request
        )
        _exec = orchestrator.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")

        hander = import_string(handler_module_path)()

        resource = hander.create_geonode_resource(
            layer_name=layer_name,
            alternate=alternate, 
            execution_id=execution_id
        )

        ResourceHandlerInfo.objects.create(
            module_path=handler_module_path,
            resource=resource
        )
        # at the end recall the import_orchestrator for the next step
        import_orchestrator.apply_async(
            (_files, execution_id, handler_module_path, step_name, layer_name, alternate)
        )
        return self.name, execution_id

    except Exception as e:
        raise ResourceCreationException(detail=error_handler(e))
