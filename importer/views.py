import logging
import pathlib
from typing import Optional

from django.contrib.auth import get_user_model
from django.utils import timezone
from geonode.base.models import ResourceBase
from geonode.layers.models import Dataset
from geonode.resource.manager import resource_manager
from geonode.resource.models import ExecutionRequest

from importer.api.exception import (InvalidInputFileException,
                                    PublishResourceException,
                                    ResourceCreationException,
                                    StartImportException)
from importer.celery_app import importer_app
from importer.celery_tasks import ErrorBaseTaskClass
from importer.datastore import DataStoreManager
from importer.orchestrator import orchestrator
from importer.publisher import DataPublisher
from importer.settings import (IMPORTER_GLOBAL_RATE_LIMIT,
                               IMPORTER_PUBLISHING_RATE_LIMIT,
                               IMPORTER_RESOURCE_CREATION_RATE_LIMIT)
from importer.utils import error_handler

logger = logging.getLogger(__name__)


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,
    name="importer.import_orchestrator",
    queue="importer.import_orchestrator",
    max_retries=1,
    rate_limit=IMPORTER_GLOBAL_RATE_LIMIT
)
def import_orchestrator(
    self,files: dict, store_spatial_files: bool = True, user: get_user_model() =None, execution_id: str =None, step='start_import', layer_name=None, alternate=None
):

    '''
    Base task. Is the task responsible to call the orchestrator and redirect the upload to the next step
    mainly is a wrapper for the Orchestrator object.

            Parameters:
                    files (dict): dictionary with the files needed for the import. it expect that there is always a base_file
                                  example: {"base_file": "/path/to/the/local/file/to/be/importerd.gpkg"}
                    store_spatial_files (bool): boolean to store spatial file or not
                    user (UserModel): user that is performing the request
                    execution_id (UUID): unique ID used to keep track of the execution request
                    step (str): last step performed from the tasks
                    layer_name (str): layer name
                    alternate (str): alternate used to naming the layer
            Returns:
                    None
    '''
    try:
        file_ext = pathlib.Path(files.get("base_file")).suffix[1:]
        # extract the resource_type of the layer and retrieve the expected handler
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
            resource_type=file_ext,
            execution_id=execution_id,
            step=step,
            layer_name=layer_name,
            alternate=alternate
        )
    except Exception as e:
        raise StartImportException(detail=error_handler(e))


@importer_app.task(
    bind=True,
    base=ErrorBaseTaskClass,    
    name="importer.import_resource",
    queue="importer.import_resource",
    max_retries=2,
    rate_limit=IMPORTER_GLOBAL_RATE_LIMIT
)
def import_resource(self, execution_id, /, resource_type):  
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
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="import_resource",
            step="importer.import_resource",
        )
        _exec = orchestrator.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")

        # initiating the data store manager
        _datastore = DataStoreManager(_files, resource_type, _exec.user, execution_id)

        # starting file validation
        if not _datastore.input_is_valid():
            raise Exception("dataset is invalid")

        _datastore.start_import(execution_id)

        '''
        The orchestrator to proceed to the next step, should be called by the hander
        since the call to the orchestrator can changed based on the handler
        called. See the GPKG handler gpkg_next_step task
        '''
        return

    except Exception as e:
        raise InvalidInputFileException(detail=error_handler(e))


@importer_app.task(
    base=ErrorBaseTaskClass,    
    name="importer.publish_resource",
    queue="importer.publish_resource",
    max_retries=1,
    rate_limit=IMPORTER_PUBLISHING_RATE_LIMIT
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
    Task to publish the resources in geoserver.
    NOTE: If the layer should be overwritten, for now we are skipping this feature
        geoserver is not ready yet

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
        
        # for now we dont heve the overwrite option in GS, skipping will we talk with the GS team
        if not _overwrite:
            _publisher = DataPublisher()
            # extracting the crs and the resource name, are needed for publish the resource
            _metadata = _publisher.extract_resource_name_and_crs(_files, resource_type, layer_name, alternate)
            if _metadata and _metadata[0].get("crs"):
                # we should not publish resource without a crs
                _, workspace, store = _publisher.publish_resources(_metadata)

                # updating the execution request status
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
        raise PublishResourceException(detail=error_handler(e))


@importer_app.task(
    base=ErrorBaseTaskClass,
    name="importer.create_gn_resource",
    queue="importer.create_gn_resource",
    max_retries=1,
    rate_limit=IMPORTER_RESOURCE_CREATION_RATE_LIMIT
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
            status=ExecutionRequest.STATUS_RUNNING,
            last_updated=timezone.now(),
            func_name="create_gn_resource",
            step="importer.create_gn_resource",
        )
        _exec = orchestrator.get_execution_object(execution_id)

        _files = _exec.input_params.get("files")
        _store_spatial_files = _exec.input_params.get("store_spatial_files")

        metadata_uploaded = _files.get("xml_file", "") or False
        sld_uploaded = _files.get("sld_file", "") or False
        _user = _exec.user

        orchestrator.update_execution_request_status(
            status=ExecutionRequest.STATUS_RUNNING,
            execution_id=execution_id,
            last_updated=timezone.now(),
            log=f"Creating GN dataset for resource: {alternate}:"
        )
    
        saved_dataset = Dataset.objects.filter(alternate__icontains=alternate)
        # if the layer exists, we just update the information of the dataset by
        # let it recreate the catalogue
        if saved_dataset.exists():
            saved_dataset = saved_dataset.first()
        else:
            # if it not exists, we create it from scratch
            if not saved_dataset.exists() and _exec.input_params.get("override_existing_layer", False):
                logger.warning(f"The dataset required {alternate} does not exists, but an overwrite is required, the resource will be created")
            saved_dataset = resource_manager.create(
                None,
                resource_type=Dataset,
                defaults=dict(
                    name=alternate,
                    workspace=_exec.input_params.get("workspace", "geonode"),
                    store=_exec.input_params.get("store", "geonode_data"),
                    subtype='vector',
                    alternate=f'{_exec.input_params.get("workspace", "geonode")}:{alternate}',
                    dirty_state=True,
                    title=layer_name,
                    owner=_user,
                    files=_files,
                )
            )

        if metadata_uploaded and resource_type != 'gpkg':
            resource_manager.update(None,
                instance=saved_dataset,
                xml_file=_files.get("xml_file", ""),
                metadata_uploaded=metadata_uploaded,
                vals={"dirty_state": True}
            )
        if sld_uploaded and resource_type != 'gpkg':
            resource_manager.exec(
                'set_style',
                None,
                instance=saved_dataset,
                sld_uploaded=sld_uploaded,
                sld_file=_files.get("sld_file", ""),
                vals={"dirty_state": True}
            )

        resource_manager.set_thumbnail(None, instance=saved_dataset)

        ResourceBase.objects.filter(alternate=alternate).update(dirty_state=False)

        # at the end recall the import_orchestrator for the next step
        import_orchestrator.apply_async(
            (_files, _store_spatial_files, _user.username, execution_id, step_name, layer_name, alternate)
        )

    except Exception as e:
        raise ResourceCreationException(detail=error_handler(e))
