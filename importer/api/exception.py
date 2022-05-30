from rest_framework import status
from rest_framework.exceptions import APIException
from django.utils.translation import ugettext_lazy as _


class ImportException(APIException):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    default_detail = 'Exception during resource upload'
    default_code = 'importer_exception'
    category = 'importer'
