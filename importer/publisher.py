import logging
import os
from typing import List

from geonode import settings
from geonode.geoserver.helpers import create_geoserver_db_featurestore
from geonode.services.serviceprocessors.base import \
    get_geoserver_cascading_workspace
from geoserver.catalog import Catalog
from geonode.utils import OGC_Servers_Handler
from osgeo import ogr

logger = logging.getLogger(__name__)


class DataPublisher():
    '''
    Given a list of resources, will publish them on GeoServer
    '''
    def __init__(self) -> None:
        ogc_server_settings = OGC_Servers_Handler(settings.OGC_SERVER)['default']
        _user, _password = ogc_server_settings.credentials

        self.cat = Catalog(
            service_url=ogc_server_settings.rest,
            username=_user,
            password=_password
        )
        self.workspace = get_geoserver_cascading_workspace(create=True)

    def extract_resource_name_and_crs(self, files: dict, resource_type: str, layer_name, alternate=None):
        '''
        Will try to extract the layers name from the original file
        this is needed since we have to publish the resources
        on geoserver by name:
        expected output:
        [
            {'name': 'layer_name', 'crs': 'EPSG:25832'}
        ]
        '''
        if resource_type == 'gpkg':
            layers = ogr.Open(files.get("base_file"))
            if not layers:
                return []
            return [
                {
                    "name": alternate or layer_name,
                    "crs" : (
                        f"{_l.GetSpatialRef().GetAuthorityName(None)}:{_l.GetSpatialRef().GetAuthorityCode('PROJCS')}"
                        if _l.GetSpatialRef() else None
                    )
                } 
                for _l in layers
                if _l.GetName() == layer_name
            ]
        return files.values() if isinstance(files, dict) else files


    def publish_resources(self, resources: List[str]):
        '''
        Given a list of strings (which rappresent the table on geoserver)
        Will publish the resorces on geoserver
        '''
        self.get_or_create_store()
        for _resource in resources:
            try:
                self.cat.publish_featuretype(
                    name=_resource.get("name"),
                    store=self.store,
                    native_crs=_resource.get("crs"),
                    srs=_resource.get("crs"),
                    jdbc_virtual_table=_resource.get("name")
                )
            except Exception as e:
                if f"Resource named {_resource.get('name')} already exists in store:" in str(e):
                    continue
                raise e
        return True, self.workspace.name, self.store.name
    
    def get_or_create_store(self):
        '''
        Evaluate if the store exists. if not is created
        '''
        geodatabase = os.environ.get('GEONODE_GEODATABASE', 'geonode_data')
        self.store = self.cat.get_store(
            name=geodatabase,
            workspace=self.workspace
        )
        if not self.store:
            logger.warning(f"The store does not exists: {geodatabase} creating...")
            self.store = create_geoserver_db_featurestore(store_name=geodatabase, workspace=self.workspace.name)
