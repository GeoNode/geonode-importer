import logging

from django.db import models
from django.db.models.signals import post_delete
from django.dispatch import receiver
from dynamic_models.models import ModelSchema
from geonode.layers.models import Dataset
from geonode.base.models import ResourceBase
from dynamic_models.schema import ModelSchemaEditor


logger = logging.getLogger(__name__)


@receiver(post_delete, sender=Dataset)
def delete_dynamic_model(instance, sender, **kwargs):
    """
    Delete the dynamic relation and the geoserver layer
    """
    try:
        name = instance.alternate.split(":")[1]
        schema = ModelSchema.objects.filter(name=name).first()
        if schema:
            '''
            We use the schema editor directly, because the model itself is not managed
            on creation, but for the delete since we are going to handle, we can use it
            '''
            _model_editor = ModelSchemaEditor(initial_model=name, db_name=schema.db_name)
            _model_editor.drop_table(schema.as_model())
            schema.delete()
        # Removing Field Schema
    except Exception as e:
        logger.error(f"Error during deletion of Dynamic Model schema: {e.args[0]}")


class ResourceHandlerInfo(models.Model):

    """
    Here we save the relation between the geonode resource created and the handler that created that resource
    """

    resource = models.ForeignKey(
        ResourceBase, blank=False, null=False, on_delete=models.CASCADE
    )
    handler_module_path = models.CharField(max_length=250, blank=False, null=False)
