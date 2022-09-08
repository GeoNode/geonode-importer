from geonode.upload.api.urls import urlpatterns
from importer.api.views import ResourceImporter, ImporterViewSet
from django.conf.urls import url

urlpatterns.insert(
    0,
    url(
        r"uploads/upload",
        ImporterViewSet.as_view({"post": "create"}),
        name="importer_upload",
    ),
)

urlpatterns.insert(
    1,
    url(
        r"resources/(?P<pk>\w+)/copy",
        ResourceImporter.as_view({"put": "copy"}),
        name="importer_resource_copy",
    ),
)
