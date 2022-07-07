import logging
from uuid import UUID

from celery import Task
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
        from importer.views import orchestrator
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