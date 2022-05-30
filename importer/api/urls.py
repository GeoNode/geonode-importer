from geonode.upload.api.urls import urlpatterns
from importer.api.views import UploadGPKGViewSet
from django.conf.urls import url

urlpatterns.insert(0,  url(r'uploads/upload', UploadGPKGViewSet.as_view({"post": "create"})))