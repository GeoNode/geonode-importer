from geonode.upload.views import view
from geonode.upload.api.views import UploadViewSet
from rest_framework.decorators import api_view


@api_view(['POST'])
def start_view(request, step=None):
    # if is a geopackage we just use the new import flow
    y = UploadViewSet()
    return y.upload(request)
