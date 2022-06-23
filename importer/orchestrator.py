import logging
import os
from uuid import UUID

from django.contrib.auth import get_user_model
from django.utils import timezone
from geonode.base.enumerations import (STATE_INVALID, STATE_PROCESSED,
                                       STATE_RUNNING)
from geonode.resource.models import ExecutionRequest
from geonode.upload.models import Upload

from importer.api.exception import ImportException
from importer.celery_app import importer_app
from importer.handlers.gpkg.handler import GPKGFileHandler

logger = logging.getLogger(__name__)


SUPPORTED_TYPES = {
    "gpkg": GPKGFileHandler()
    # "vector": VectorFileHandler
}


class ImportOrchestrator:
    ''''
    Main import object. Is responsible to handle all the execution steps
    Using the ExecutionRequest object, will extrapolate the information and
    it call the next step of the import pipeline
    Params: 
    
    enable_legacy_upload_status default=True: if true, will save the upload progress
        also in the legacy upload system
    '''
    def __init__(self, enable_legacy_upload_status=True) -> None:
        self.enable_legacy_upload_status = enable_legacy_upload_status

    @property
    def supported_type(self):
        """
        Returns the supported types for the import
        """
        return SUPPORTED_TYPES.keys()

    def get_file_handler(self, file_type):
        """
        Returns the supported types for the import
        """
        _type = SUPPORTED_TYPES.get(file_type)
        if not _type:
            raise ImportException(
                detail=f"The requested filetype is not supported: {file_type}"
            )
        return _type

    def get_execution_object(self, exec_id):
        '''
        Returns the ExecutionRequest object with the detail about the 
        current execution
        '''
        req = ExecutionRequest.objects.filter(exec_id=exec_id)
        if not req.exists():
            raise ImportException("The selected UUID does not exists")
        return req.first()

    def perform_next_import_step(self, resource_type: str, execution_id: str, step: str = None, layer_name:str = None, alternate:str = None) -> None:
        '''
        It takes the executionRequest detail to extract which was the last step
        and take from the task_lists provided by the ResourceType handler
        which will be the following step. if empty a None is returned, otherwise
        in async the next step is called
        '''
        try:
            if step is None:
                step = self.get_execution_object(str(execution_id)).step

            # retrieve the task list for the resource_type
            tasks = self.get_file_handler(resource_type).TASKS_LIST
            # getting the index
            _index = tasks.index(step) + 1
            # finding in the task_list the last step done
            remaining_tasks = tasks[_index:] if not _index >= len(tasks) else []
            if not remaining_tasks:
                # The list of task is empty, it means that the process is finished
                self.set_as_completed(execution_id)
                return
            # getting the next step to perform
            next_step = next(iter(remaining_tasks))
            # calling the next step for the resource

            # defining the tasks parameter for the step
            task_params = (str(execution_id), resource_type)
            logger.error(f"STARTING NEXT STEP {next_step}")

            if layer_name and alternate:
                # if the layer and alternate is provided, means that we are executing the step specifically for a layer
                # so we add this information to the task_parameters to be sent to the next step
                logger.error(f"STARTING NEXT STEP {next_step} for resource: {layer_name}, alternate {alternate}")

                '''
                If layer name and alternate are provided, are sent as an argument
                for the next task step
                '''
                task_params = (
                        str(execution_id),
                        resource_type,
                        next_step,
                        layer_name,
                        alternate
                    )

            # continuing to the next step
            importer_app.tasks.get(next_step).apply_async(task_params)

        except StopIteration:
            # means that the expected list of steps is completed
            logger.info("The whole list of tasks has been processed")
            self.set_as_completed(execution_id)
            return
        except Exception as e:
            self.set_as_failed(execution_id, reason=e.args[0])
            raise e

    def set_as_failed(self, execution_id, reason=None):
        '''
        Utility method to set the ExecutionRequest object to fail
        '''
        self.update_execution_request_status(
                execution_id=str(execution_id),
                status=ExecutionRequest.STATUS_FAILED,
                finished=timezone.now(),
                last_updated=timezone.now(),
                log=reason,
                legacy_status=STATE_INVALID
            )

    def set_as_completed(self, execution_id):
        '''
        Utility method to set the ExecutionRequest object to fail
        '''
        self.update_execution_request_status(
                execution_id=str(execution_id),
                status=ExecutionRequest.STATUS_FINISHED,
                finished=timezone.now(),
                last_updated=timezone.now(),
                legacy_status=STATE_PROCESSED
            )

    def create_execution_request(
        self,
        user: get_user_model,
        func_name: str,
        step: str,
        input_params: dict,
        resource=None,
        legacy_upload_name=""
    ) -> UUID:
        """
        Create an execution request for the user. Return the UUID of the request
        """
        execution = ExecutionRequest.objects.create(
            user=user,
            geonode_resource=resource,
            func_name=func_name,
            step=step,
            input_params=input_params,
        )
        if self.enable_legacy_upload_status:
            # getting the package name from the base_filename
            Upload.objects.create(
                name=legacy_upload_name or os.path.basename(input_params.get("files", {}).get("base_file")),
                state=STATE_RUNNING,
                metadata={
                    **{
                        "func_name": func_name,
                        "step": step,
                        "exec_id": str(execution.exec_id),
                    },
                    **input_params,
                },
            )
        return execution.exec_id

    def update_execution_request_status(self, execution_id, status, legacy_status=STATE_RUNNING, **kwargs):
        '''
        Update the execution request status and also the legacy upload status if the
        feature toggle is enabled
        '''
        ExecutionRequest.objects.filter(exec_id=execution_id).update(
            status=status, **kwargs
        )
        if self.enable_legacy_upload_status:
            Upload.objects.filter(metadata__contains=execution_id).update(
                state=legacy_status, complete=True, metadata={**kwargs, **{"exec_id": execution_id}}
            )

orchestrator = ImportOrchestrator()
