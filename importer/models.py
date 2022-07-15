from dynamic_models.models import FieldSchema, ModelSchema
from geonode.geoserver.signals import geoserver_delete
import logging
from django.db.models.signals import post_delete
from django.dispatch import receiver
from geonode.layers.models import Dataset

logger = logging.getLogger(__name__)


@receiver(post_delete, sender=Dataset)
def delete_dynamic_model(instance, sender, **kwargs):
    '''
    Delete the dynamic relation and the geoserver layer
    '''
    try:
        geoserver_delete(instance.alternate)
        name = instance.alternate.split(":")[1]
        ModelSchema.objects.filter(name=name).delete()
        FieldSchema.objects.filter(name=name).delete()
        # Removing Field Schema
    except Exception as e:
        logger.error(f"Error during deletion of Dynamic Model schema: {e.args[0]}")
