from rest_framework import serializers
from dynamic_rest.serializers import DynamicModelSerializer
from geonode.upload.models import Upload


class RemoteResourceSerializer(DynamicModelSerializer):
    class Meta:
        ref_name = "RemoteResourceSerializer"
        model = Upload
        view_name = "importer_upload"
        fields = (
            "url",
            "title",
            "type",
            "source",
        )

    url = serializers.URLField(required=True)
    title = serializers.CharField(required=False)
    type = serializers.CharField(required=True)
    source = serializers.CharField(required=False, default="upload")
