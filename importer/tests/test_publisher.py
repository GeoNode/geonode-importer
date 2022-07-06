
from django.test import TestCase

from importer.publisher import DataPublisher


class TestPublisher(TestCase):
    '''
    Test to get the information and publish the resource in geoserver
    '''
    def setUpClass(cls):
        cls.publisher = DataPublisher()

    def test_extract_resource_name_and_crs():
        '''
        Given a layer and the original file, should extract the crs and the name
        to let it publish in Geoserver
        '''