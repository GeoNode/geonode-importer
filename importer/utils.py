from geonode.resource.manager import ResourceManager
from geonode.geoserver.manager import ResourceManagerInterface

def error_handler(exc):
    return str(exc.detail if hasattr(exc, "detail") else exc.args[0])


class ImporterConcreteManager(ResourceManagerInterface):
    pass

importer_resource_manager = ResourceManager(concrete_manager=ImporterConcreteManager)
