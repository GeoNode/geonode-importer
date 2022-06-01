from abc import ABC


STANDARD_TYPE_MAPPING = {
    "Integer64": "django.db.models.IntegerField",
    "Integer": "django.db.models.IntegerField",
    "DateTime": "django.db.models.DateTimeField",
    "Real": "django.db.models.FloatField",
    "String": "django.db.models.CharField"
}

GEOM_TYPE_MAPPING = {
    "Multi Line String": "django.contrib.gis.db.models.MultiLineStringField",
    "Point": "django.contrib.gis.db.models.PointField",
    "Polygon": "django.contrib.gis.db.models.PolygonField",
    "Multi Point": "django.contrib.gis.db.models.MultiPointField"
}


class AbstractHandler(ABC):
    TASKS_LIST = []

    def step_list(self):
        return self.TASKS_LIST

    def is_valid(self):
        """
        Define basic validation steps
        """
        return NotImplementedError

    def start_import(self):
        '''
        Define the step to perform the import of the data
        into the datastore db
        '''
        return NotImplementedError

