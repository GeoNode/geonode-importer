import logging
import os
from typing import List

from geonode import settings
from geonode.geoserver.helpers import create_geoserver_db_featurestore
from geonode.services.serviceprocessors.base import get_geoserver_cascading_workspace
from geoserver.catalog import Catalog
from geonode.utils import OGC_Servers_Handler
from django.utils.module_loading import import_string

from importer.api.exception import PublishResourceException


logger = logging.getLogger(__name__)


class DataPublisher:
    """
    Given a list of resources, will publish them on GeoServer
    """

    def __init__(self, handler_module_path) -> None:
        ogc_server_settings = OGC_Servers_Handler(settings.OGC_SERVER)["default"]

        _user, _password = ogc_server_settings.credentials

        self.cat = Catalog(
            service_url=ogc_server_settings.rest, username=_user, password=_password
        )
        self.workspace = get_geoserver_cascading_workspace(create=True)

        self.handler = import_string(handler_module_path)()

    def extract_resource_to_publish(
        self, files: dict, action: str, layer_name, alternate=None, **kwargs
    ):
        """
        Will try to extract the layers name from the original file
        this is needed since we have to publish the resources
        on geoserver by name:
        expected output:
        [
            {'name': 'layer_name', 'crs': 'EPSG:25832'}
        ]
        """

        return self.handler.extract_resource_to_publish(
            files, action, layer_name, alternate, **kwargs
        )

    def get_resource(self, resource_name) -> bool:
        self.get_or_create_store()
        _res = self.cat.get_resource(
            resource_name, store=self.store, workspace=self.workspace
        )
        return True if _res else False

    def publish_resources(self, resources: List[str]):
        """
        Given a list of strings (which rappresent the table on geoserver)
        Will publish the resorces on geoserver
        """
        self.get_or_create_store()
        result = self.handler.publish_resources(
            resources=resources,
            catalog=self.cat,
            store=self.store,
            workspace=self.workspace,
        )
        self.sanity_checks(resources)
        return result

    def delete_resource(self, resource_name):
        layer = self.get_resource(resource_name)
        if layer:
            self.cat.delete(layer.resource, purge="all", recurse=True)

    def get_resource(self, dataset_name):
        return self.cat.get_layer(dataset_name)

    def overwrite_resources(self, resources: List[str]):
        """
        Not available for now, waiting geoserver 2.20/2.21 available with Geonode
        """
        pass

    def get_or_create_store(self):
        """
        Evaluate if the store exists. if not is created
        """
        geodatabase = os.environ.get("GEONODE_GEODATABASE", "geonode_data")
        self.store = self.cat.get_store(name=geodatabase, workspace=self.workspace)
        if not self.store:
            logger.warning(f"The store does not exists: {geodatabase} creating...")
            self.store = create_geoserver_db_featurestore(
                store_name=geodatabase, workspace=self.workspace.name
            )

    def publish_geoserver_view(
        self, layer_name, crs, view_name, sql=None, geometry=None
    ):
        """
        Let the handler create a geoserver view given the input parameters
        """
        self.get_or_create_store()

        return self.handler.publish_geoserver_view(
            catalog=self.cat,
            workspace=self.workspace,
            datastore=self.store,
            layer_name=layer_name,
            crs=crs,
            view_name=view_name,
            sql=sql,
            geometry=geometry,
        )

    def sanity_checks(self, resources):
        """
        Will evaluate if the SRID is correctly created
        For each resource. This is a quick test to be sure
        that the resource is correctly set/created
        """
        for _resource in resources:
            res = self.cat.get_resource(_resource.get("name"), workspace=self.workspace)
            if not res.projection:
                raise PublishResourceException(
                    f"The SRID for the resource {res.name} is not correctly set, Please check Geoserver logs"
                )
