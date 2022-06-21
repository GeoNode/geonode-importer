from rest_framework import serializers


class ImporterSerializer(serializers.Serializer):
    class Meta:
        fields = (
            "base_file", "dbf_file", "shx_file", "prj_file", "xml_file",
            "sld_file", "store_spatial_files", "override_existing_layer",
            "skip_existsing_layer"
        )

    base_file = serializers.FileField()
    dbf_file = serializers.CharField(required=False)
    shx_file = serializers.CharField(required=False)
    prj_file = serializers.CharField(required=False)
    xml_file = serializers.CharField(required=False)
    sld_file = serializers.CharField(required=False)
    store_spatial_files = serializers.BooleanField(required=False, default=True)
    override_existing_layer = serializers.BooleanField(required=False, default=False)
    skip_existsing_layer = serializers.BooleanField(required=False, default=False)
