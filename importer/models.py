import logging

from django.db import models
from django.db.models.signals import post_delete, pre_delete
from django.dispatch import receiver
from geonode.geoserver.signals import geoserver_delete
from geonode.layers.models import Dataset
from geonode.base.models import ResourceBase
from importer.orchestrator import orchestrator


logger = logging.getLogger(__name__)


@receiver(pre_delete, sender=Dataset)
def delete_dynamic_model(instance, sender, **kwargs):
    """
    Delete the dynamic relation and the geoserver layer
    """
    try:
        #geoserver_delete(instance.alternate)
        if instance.resourcehandlerinfo_set.exists():
            handler_module_path = (
                instance.resourcehandlerinfo_set.first().handler_module_path
            )
            handler = orchestrator.load_handler(handler_module_path)
            handler.delete_resource(instance)
        # Removing Field Schema
    except Exception as e:
        logger.error(f"Error during deletion instance deletion: {e.args[0]}")


class ResourceHandlerInfo(models.Model):

    """
    Here we save the relation between the geonode resource created and the handler that created that resource
    """

    resource = models.ForeignKey(
        ResourceBase, blank=False, null=False, on_delete=models.CASCADE
    )
    handler_module_path = models.CharField(max_length=250, blank=False, null=False)
    kwargs = models.JSONField(verbose_name="Storing strictly related information of the handler", default=dict)
