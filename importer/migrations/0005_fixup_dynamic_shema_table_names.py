from django.db import migrations


def fixup_table_name(apps, schema_editor):
    schema = apps.get_model('dynamic_models', 'ModelSchema')
    for val in schema.objects.all():
        if val.name != val.db_table_name:
            val.db_table_name = val.name
            val.save()


class Migration(migrations.Migration):

    dependencies = [
        ('importer', '0004_rename_execution_id_resourcehandlerinfo_execution_request'),
    ]

    operations = [
        migrations.RunPython(fixup_table_name),
    ]
