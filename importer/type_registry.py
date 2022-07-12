from importer.handlers.gpkg.handler import GPKGFileHandler
import logging 

logger = logging.getLogger(__name__)


class SupportedTypeRegistry(type):

    REGISTRY = {}

    def __new__(cls, name, bases, attrs):
        # instantiate a new type corresponding to the type of class being defined
        # this is currently RegisterBase but in child classes will be the child class
        new_cls = type.__new__(cls, name, bases, attrs)

        if new_cls.__name__.lower() in cls.REGISTRY:
            _log =  f"The class with name {new_cls.__name__.lower()} is already present in the registry, Please take another name"
            logger.error(_log)
            raise Exception(_log)
        elif not attrs.get("handler"):
            raise Exception("If must define the handler")

        cls.REGISTRY[new_cls.__name__.lower()] = attrs.get("handler")
        return new_cls


class GPKG(metaclass=SupportedTypeRegistry):
    handler = GPKGFileHandler()


SUPPORTED_TYPES = SupportedTypeRegistry.REGISTRY