from geonode.upload.api.urls import urlpatterns
from importer.api.views import ImporterViewSet
from django.conf.urls import url

urlpatterns.insert(
    0, 
    url(r'uploads/upload', ImporterViewSet.as_view({"post": "create"}), name="importer_upload")
)