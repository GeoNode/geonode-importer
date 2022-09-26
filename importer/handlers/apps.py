import logging

from django.apps import AppConfig
from django.conf import settings
from django.utils.module_loading import import_string


logger = logging.getLogger(__name__)


class HandlersConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "importer.handlers"

    def ready(self):
        """Finalize setup"""
        run_setup_hooks()
        super(HandlersConfig, self).ready()


def run_setup_hooks(*args, **kwargs):
    if getattr(settings, 'IMPORTER_HANDLERS', []):
        _handlers = [import_string(module_path) for module_path in settings.IMPORTER_HANDLERS]
        list(map(lambda item: item.register(), _handlers))
        logger.info(f"The following handlers have been registered: {', '.join(settings.IMPORTER_HANDLERS)}")
        
