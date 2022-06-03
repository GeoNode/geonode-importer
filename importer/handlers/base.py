from abc import ABC


STANDARD_TYPE_MAPPING = {
    "Integer64": "django.db.models.IntegerField",
    "Integer": "django.db.models.IntegerField",
    "DateTime": "django.db.models.DateTimeField",
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


class AbstractHandler(ABC):
    '''
    Base abstract handler object
    define the required method needed to define an upload handler
    it must be:
    - provide the tasks list to complete the import
    - validation function
    - method to import the resource
    '''
    TASKS_LIST = []

    def step_list(self):
        return self.TASKS_LIST

    def is_valid(self):
        """
        Define basic validation steps
        """
        return NotImplementedError

    def import_resource(self):
        '''
        Define the step to perform the import of the data
        into the datastore db
        '''
        return NotImplementedError

