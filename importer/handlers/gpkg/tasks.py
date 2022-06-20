import logging

from celery import Task
from geonode.resource.models import ExecutionRequest
logger = logging.getLogger(__name__)


class SingleMessageErrorHandler(Task):

    max_retries = 1
    track_started=True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # exc (Exception) - The exception raised by the task.
        # args (Tuple) - Original arguments for the task that failed.
        # kwargs (Dict) - Original keyword arguments for the task that failed.
        from importer.views import orchestrator

        exec_id = orchestrator.get_execution_object(exec_id=args[0])
        output_params = exec_id.output_params.copy()

        logger.error(f"Task FAILED with ID: {args[1]}, reason: {exc}")
        _log = orchestrator.get_file_handler('gpkg').create_error_log(self.name, *args)
        if output_params.get("errors"):
            output_params.get("errors").append(_log)
        else:
            output_params = {"errors": [_log]}

        orchestrator.update_execution_request_status(
            execution_id=args[0],
            status=ExecutionRequest.STATUS_FAILED,
            
            output_params=output_params,
            log=str(exc.detail if hasattr(exc, "detail") else exc.args[0])
        )
