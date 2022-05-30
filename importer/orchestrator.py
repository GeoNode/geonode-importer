from django.contrib.auth import get_user_model
from geonode.resource.models import ExecutionRequest
from importer.api.exception import ImportException
from importer.handlers import GPKGStepHandler


class ImportOrchestrator:
    SUPPORTED_TYPES = {
        "gpkg": GPKGStepHandler
        #"vector": VectorFileHandler
    }

    def __init__(self, enable_legacy_upload_status=True) -> None:
        self.enable_legacy_upload_status = enable_legacy_upload_status

    @property
    def get_supported_type_list(self):
        '''
        Returns the supported types for the import
        '''
        return self.SUPPORTED_TYPES.keys()

    def get_file_handler(self, file_type):
        '''
        Returns the supported types for the import
        '''
        return self.SUPPORTED_TYPES.get(file_type)

    def perform_next_import_step(self, resource_type, execution_id):
        # Getting the execution object
        _exec = self._get_execution_object(str(execution_id))
        # retrieve the task list for the resource_type
        tasks = self.get_file_handler(resource_type).TASKS_LIST
        # getting the index
        _index = tasks.index(_exec.input_params.get('step'))+1
        # finding in the task_list the last step done
        remaining_tasks = tasks[_index:] if not _index > len(tasks) else []
        # getting the next step to perform
        next_step = next(iter(remaining_tasks))
        if not next_step:
            return
        from importer.views import app as celery_app
        print(celery_app)
        pass


    def create_execution_request(self, user: get_user_model, func_name: str, input_params: dict, resource=None) -> str:
        '''
        Create an execution request for the user. Return the UUID of the request
        '''
        execution = ExecutionRequest.objects.create(
            user=user,
            geonode_resource=resource,
            func_name=func_name,
            input_params=input_params
        )
        return execution.exec_id
    
    def update_execution_request(self, execution_id, **kwargs):
        ExecutionRequest.objects.update(uuid=execution_id).update(**kwargs)

    def _get_execution_object(self, exec_id):
        req = ExecutionRequest.objects.filter(exec_id=exec_id).first()
        if req is None:
            raise ImportException("The selected UUID does not exists")
        return req