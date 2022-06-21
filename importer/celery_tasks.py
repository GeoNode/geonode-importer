import logging

from celery import Task
logger = logging.getLogger(__name__)


class ErrorBaseTaskClass(Task):

    max_retries = 3
    track_started=True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # exc (Exception) - The exception raised by the task.
        # args (Tuple) - Original arguments for the task that failed.
        # kwargs (Dict) - Original keyword arguments for the task that failed.
        from importer.views import orchestrator

        logger.error(f"Task FAILED with ID: {args[0]}, reason: {exc}")
        orchestrator.set_as_failed(
            execution_id=args[0], reason=str(exc.detail if hasattr(exc, "detail") else exc.args[0])
        )
