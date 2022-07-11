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
import os
import pathlib

from django.utils.translation import ugettext as _
from dynamic_rest.filters import DynamicFilterBackend, DynamicSortingFilter
from dynamic_rest.viewsets import DynamicModelViewSet
from geonode.base.api.filters import DynamicSearchFilter
from geonode.base.api.pagination import GeoNodeApiPagination
from geonode.base.api.permissions import IsOwnerOrReadOnly
from geonode.storage.manager import StorageManager
from geonode.upload.api.permissions import UploadPermissionsFilter
from geonode.upload.api.views import UploadViewSet
from geonode.upload.models import Upload
from importer.api.exception import ImportException
from importer.api.serializer import ImporterSerializer
from importer.views import import_orchestrator, orchestrator
from oauth2_provider.contrib.rest_framework import OAuth2Authentication
from rest_framework.authentication import (BasicAuthentication,
                                           SessionAuthentication)
from rest_framework.parsers import FileUploadParser, MultiPartParser
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from geonode.upload.utils import UploadLimitValidator
import ast

logger = logging.getLogger(__name__)


class ImporterViewSet(DynamicModelViewSet):
    """
    API endpoint that allows uploads to be viewed or edited.
    """
    parser_class = [FileUploadParser, MultiPartParser]

    authentication_classes = [BasicAuthentication, SessionAuthentication, OAuth2Authentication]
    permission_classes = [IsAuthenticatedOrReadOnly, IsOwnerOrReadOnly]
    filter_backends = [
        DynamicFilterBackend, DynamicSortingFilter, DynamicSearchFilter,
        UploadPermissionsFilter
    ]
    queryset = Upload.objects.all()
    serializer_class = ImporterSerializer
    pagination_class = GeoNodeApiPagination
    http_method_names = ['get', 'post']

    def create(self, request, *args, **kwargs):
        '''
        Main function called by the new import flow.
        It received the file via the front end
        if is a gpkg (in future it will support all the vector file)
        the new import flow is follow, else the normal upload api is used.
        It clone on the local repo the file that the user want to upload
        '''
        _file = request.FILES.get('base_file') or request.data.get('base_file')
        if _file and (_file.name.endswith('gpkg') if hasattr(_file, 'name') else _file.endswith('gpkg')):
            file_ext = pathlib.Path(files.get("base_file")).suffix[1:]

            handler = orchestrator.get_file_handler(file_ext)

            #go through the new import flow
            data = self.serializer_class(data=request.data)
            # serializer data validation
            data.is_valid(raise_exception=True)
            # cloning data into a local folder
            _data = data.data.copy()
            skip_existing_layers = _data.pop('skip_existing_layers', "False")
            override_existing_layer = _data.pop('override_existing_layer', "False")
            store_spatial_file = _data.pop("store_spatial_files", "True")
            storage_manager = StorageManager(remote_files={**_data, **{"base_file": request.data.get('base_file')}})
            storage_manager.clone_remote_files()
            # get filepath
            files = storage_manager.get_retrieved_paths()

            try:
                upload_validator = UploadLimitValidator(request.user)
                upload_validator.validate_parallelism_limit_per_user()
                upload_validator.validate_files_sum_of_sizes(storage_manager.data_retriever)

                execution_id = orchestrator.create_execution_request(
                    user=request.user,
                    func_name=next(iter(handler.TASKS_LIST)),
                    step=next(iter(handler.TASKS_LIST)),
                    input_params={
                        "files": files,
                        "store_spatial_files": (
                            ast.literal_eval(store_spatial_file)
                            if isinstance(store_spatial_file, str)
                            else store_spatial_file
                        ),
                        "skip_existing_layer": (
                            ast.literal_eval(skip_existing_layers)
                            if isinstance(skip_existing_layers, str)
                            else skip_existing_layers
                        ),
                        "override_existing_layer": (
                            ast.literal_eval(override_existing_layer)
                            if isinstance(override_existing_layer, str)
                            else override_existing_layer
                        )
                    },
                    legacy_upload_name=os.path.basename(files.get("base_file"))
                )

                sig = import_orchestrator.s(
                        files,
                        data.data.get("store_spatial_files"),
                        request.user.username,
                        str(execution_id),
                )
                sig.apply_async()
                return Response(data={"execution_id": execution_id}, status=201)
            except Exception as e:
                # in case of any exception, is better to delete the 
                # cloned files to keep the storage under control
                storage_manager.delete_retrieved_paths(force=True)
                raise ImportException(detail=e.args[0])

        # if is a geopackage we just use the new import flow
        request.GET._mutable = True
        return UploadViewSet().upload(request)
