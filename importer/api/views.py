#########################################################################
#
# Copyright (C) 2021 OSGeo
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#########################################################################
import logging

from django.utils.translation import ugettext as _
from drf_spectacular.utils import extend_schema
from dynamic_rest.filters import DynamicFilterBackend, DynamicSortingFilter
from dynamic_rest.viewsets import DynamicModelViewSet
from geonode.base.api.filters import DynamicSearchFilter
from geonode.base.api.pagination import GeoNodeApiPagination
from geonode.base.api.permissions import IsOwnerOrReadOnly
from geonode.upload.api.permissions import UploadPermissionsFilter
from geonode.upload.api.serializers import UploadSerializer
from geonode.upload.api.views import UploadViewSet
from geonode.upload.models import Upload
from oauth2_provider.contrib.rest_framework import OAuth2Authentication
from rest_framework.authentication import (BasicAuthentication,
                                           SessionAuthentication)
from rest_framework.decorators import action
from rest_framework.parsers import FileUploadParser
from rest_framework.permissions import IsAuthenticatedOrReadOnly

logger = logging.getLogger(__name__)


class UploadGPKGViewSet(DynamicModelViewSet):
    """
    API endpoint that allows uploads to be viewed or edited.
    """
    parser_class = [FileUploadParser, ]

    authentication_classes = [SessionAuthentication, BasicAuthentication, OAuth2Authentication]
    permission_classes = [IsAuthenticatedOrReadOnly, IsOwnerOrReadOnly]
    filter_backends = [
        DynamicFilterBackend, DynamicSortingFilter, DynamicSearchFilter,
        UploadPermissionsFilter
    ]
    queryset = Upload.objects.all()
    serializer_class = UploadSerializer
    pagination_class = GeoNodeApiPagination
    http_method_names = ['get', 'post']

    def create(self, request, *args, **kwargs):
        _file = request.FILES.get('base_file')
        if _file and _file.name.endswith('gpkg'):
            #go through the new import flow
            return
        # if is a geopackage we just use the new import flow
        request.GET._mutable = True
        return UploadViewSet().upload(request)
