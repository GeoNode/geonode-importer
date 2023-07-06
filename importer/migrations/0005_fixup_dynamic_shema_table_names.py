from django.db import migrations
import logging
from django.db import ProgrammingError

logger = logging.getLogger(__name__)


def fixup_table_name(apps, schema_editor):
    try:
        schema = apps.get_model("dynamic_models", "ModelSchema")
        for val in schema.objects.all():
            if val.name != val.db_table_name:
                val.db_table_name = val.name
                val.save()
    except ProgrammingError as e:
        """
        The dynamic model should exists to apply the above migration.
        In case it does not exists we can skip it
        """
        if 'relation "dynamic_models_modelschema" does not exist' in e.args[0]:
            logging.debug("Dynamic model does not exists yet, skipping")
            return
        raise e
    except Exception as e:
        raise e


class Migration(migrations.Migration):
    dependencies = [
        ("importer", "0004_rename_execution_id_resourcehandlerinfo_execution_request"),
        ("dynamic_models", "0005_auto_20220621_0718"),
    ]

    operations = [
        migrations.RunPython(fixup_table_name),
    ]
