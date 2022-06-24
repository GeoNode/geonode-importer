

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
