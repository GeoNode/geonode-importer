# Generated by Django 3.2.15 on 2022-10-04 13:03

import logging
from django.db import migrations
from importer.orchestrator import orchestrator
from geonode.layers.models import Dataset
from geonode.assets.utils import get_default_asset
from geonode.utils import get_allowed_extensions

logger = logging.getLogger("django")

def dataset_migration(apps, _):
    NewResources = apps.get_model("importer", "ResourceHandlerInfo")
    for old_resource in Dataset.objects.exclude(
        pk__in=NewResources.objects.values_list("resource_id", flat=True)
    ).exclude(subtype__in=["remote", None]):
        # generating orchestrator expected data file
        if old_resource.resourcehandlerinfo_set.first() is None:
            if get_default_asset(old_resource):
                available_choices = get_allowed_extensions()
                not_main_files = ["xml", "sld", "zip", "kmz"]
                base_file_choices = set(x for x in available_choices if x not in not_main_files)
                output_files = dict()
                for _file in get_default_asset(old_resource).location:
                    if _file.split(".")[-1] in base_file_choices:
                        output_files.update({"base_file": _file})
                        break

                handler = orchestrator.get_handler(output_files)
                if handler is None:
                    logger.error(f"Handler not found for resource: {old_resource}")
                    continue
                handler.create_resourcehandlerinfo(
                    handler_module_path=str(handler),
                    resource=old_resource,
                    execution_id=None
                )   
            else:
                logger.error(f"Was not possible to generare resource_handler_info for resource: {old_resource}")  


class Migration(migrations.Migration):
    dependencies = [
        ("importer", "0006_dataset_migration"),
    ]

    operations = [
        migrations.RunPython(dataset_migration),
    ]
