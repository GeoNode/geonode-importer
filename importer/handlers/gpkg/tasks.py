import logging
from uuid import UUID

from celery import Task
from django.utils.module_loading import import_string
from django_celery_results.models import TaskResult

logger = logging.getLogger(__name__)


class SingleMessageErrorHandler(Task):

    max_retries = 1
    track_started=True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # exc (Exception) - The exception raised by the task.
        # args (Tuple) - Original arguments for the task that failed.
        # kwargs (Dict) - Original keyword arguments for the task that failed.
        from importer.celery_tasks import orchestrator

        exec_id = orchestrator.get_execution_object(exec_id=self._get_uuid(args))
        output_params = exec_id.output_params.copy()

        logger.error(f"Task FAILED with ID: {str(exec_id.exec_id)}, reason: {exc}")

        handler = import_string(exec_id.input_params.get("handler_module_path"))

        # creting the log message
        _log = handler.create_error_log(exc, self.name, *args)

        if output_params.get("errors"):
            output_params.get("errors").append(_log)
        else:
            output_params = {"errors": [_log]}

        orchestrator.update_execution_request_status(
            execution_id=args[0],
            output_params=output_params,
            log=str(exc.detail if hasattr(exc, "detail") else exc.args[0])
        )

        self.update_state(
            task_id=task_id,
            state="FAILURE",
            meta={
                "exec_id": str(exec_id.exec_id),
                "reason": _log
            }
        )
        #TaskResult.objects.filter(task_id=task_id).update(task_args=self._get_uuid(args))

        orchestrator.evaluate_execution_progress(self._get_uuid(args))

    def _get_uuid(self, _list):
        for el in _list:
            try:
                UUID(el)
                return el
            except:
                continue
