from datetime import datetime
import logging
from django.contrib.auth import get_user_model
from geonode.resource.models import ExecutionRequest
from importer.api.exception import ImportException
from importer.handlers import GPKGFileHandler

logger = logging.getLogger(__name__)


SUPPORTED_TYPES = {
    "gpkg": GPKGFileHandler()
    #"vector": VectorFileHandler
}

class ImportOrchestrator:

    def __init__(self, enable_legacy_upload_status=True) -> None:
        self.enable_legacy_upload_status = enable_legacy_upload_status

    @property
    def get_supported_type_list(self):
        '''
        Returns the supported types for the import
        '''
        return SUPPORTED_TYPES.keys()

    def get_file_handler(self, file_type):
        '''
        Returns the supported types for the import
        '''
        return SUPPORTED_TYPES.get(file_type)

    def perform_next_import_step(self, resource_type: str, execution_id: str) -> None:
        # Getting the execution object
        _exec = self._get_execution_object(str(execution_id))
        # retrieve the task list for the resource_type
        tasks = self.get_file_handler(resource_type).TASKS_LIST
        # getting the index
        try:
            _index = tasks.index(_exec.step)+1
            # finding in the task_list the last step done
            remaining_tasks = tasks[_index:] if not _index >= len(tasks) else []
            if not remaining_tasks:
                return
            # getting the next step to perform
            next_step = next(iter(remaining_tasks))
            from importer.views import app as celery_app
            # calling the next step for the resource
            celery_app.tasks.get(next_step).apply_async((resource_type, str(execution_id),))

        except StopIteration:
            # means that the expected list of steps is completed
            logger.info("The whole list of tasks has been processed")
            return
        except Exception as e:
            self.update_execution_request_status(
                execution_id=execution_id.exec_id,
                status=ExecutionRequest.STATUS_FAILED,
                finished=True,
                last_updated=datetime.utcnow()
            )            
            raise ImportException(detail=e.args[0])
        pass


    def create_execution_request(self, user: get_user_model, func_name: str, step: str, input_params: dict, resource=None) -> str:
        '''
        Create an execution request for the user. Return the UUID of the request
        '''
        execution = ExecutionRequest.objects.create(
            user=user,
            geonode_resource=resource,
            func_name=func_name,
            step=step,
            input_params=input_params
        )
        return execution.exec_id
    
    def update_execution_request_status(self, execution_id, **kwargs):
        ExecutionRequest.objects.filter(exec_id=execution_id).update(**kwargs)

    def _get_execution_object(self, exec_id):
        req = ExecutionRequest.objects.filter(exec_id=exec_id).first()
        if req is None:
            raise ImportException("The selected UUID does not exists")
        return req