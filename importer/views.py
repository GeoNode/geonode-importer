from geonode.upload.views import view
from geonode.upload.api.views import UploadViewSet
from rest_framework.decorators import api_view


@api_view(['POST'])
def start_view(request):
    _file = request.FILES.get('base_file')
    if _file and _file.name.endswith('gpkg'):
        #go through the new import flow
        return
    # if is a geopackage we just use the new import flow
    request.GET._mutable = True
    return UploadViewSet().upload(request)
