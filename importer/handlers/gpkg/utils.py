import hashlib
from dynamic_models.schema import ModelSchemaEditor
import logging

logger = logging.getLogger(__name__)


STANDARD_TYPE_MAPPING = {
    "Integer64": "django.db.models.IntegerField",
    "Integer": "django.db.models.IntegerField",
    "DateTime": "django.db.models.DateTimeField",
    "Date": "django.db.models.DateField",
    "Real": "django.db.models.FloatField",
    "String": "django.db.models.CharField"
}

GEOM_TYPE_MAPPING = {
    "Line String": "django.contrib.gis.db.models.fields.LineStringField",
    "Multi Line String": "django.contrib.gis.db.models.fields.MultiLineStringField",
    "Point": "django.contrib.gis.db.models.fields.PointField",
    "Polygon": "django.contrib.gis.db.models.fields.PolygonField",
    "Multi Point": "django.contrib.gis.db.models.fields.MultiPointField",
    "Multi Polygon": "django.contrib.gis.db.models.fields.MultiPolygonField",
}


def drop_dynamic_model_schema(schema_model):
    if schema_model:
        schema = ModelSchemaEditor(
            initial_model=schema_model.name,
            db_name="datastore"
        )
        try:
            schema.drop_table(schema_model.as_model())
        except Exception as e:
            logger.warning(e.args[0])

        schema_model.delete()


def create_alternate(layer_name, execution_id):
    '''
    Utility to generate the expected alternate for the resource
    is alternate = layer_name_ + md5(layer_name + uuid)
    '''
    _hash = hashlib.md5(
        f"{layer_name}_{execution_id}".encode('utf-8')
    ).hexdigest()
    alternate = f"{layer_name}_{_hash}"
    if len(alternate) >= 64: # 64 is the max table lengh in postgres
        return f"{layer_name[:50]}{_hash[:14]}"
    return alternate
