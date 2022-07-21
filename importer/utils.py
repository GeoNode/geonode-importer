from geonode.resource.manager import ResourceManager
from geonode.geoserver.manager import GeoServerResourceManager
from geonode.base.models import ResourceBase

def error_handler(exc):
    return str(exc.detail if hasattr(exc, "detail") else exc.args[0])


class ImporterConcreteManager(GeoServerResourceManager):

    def copy(self, instance, uuid, defaults):
        return ResourceBase.objects.get(uuid=uuid)

    def update(self, uuid, **kwargs) -> ResourceBase:
        return ResourceBase.objects.get(uuid=uuid)

custom_resource_manager = ResourceManager(concrete_manager=ImporterConcreteManager())
