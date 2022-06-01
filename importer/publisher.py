from typing import List
from geoserver.catalog import Catalog
from geonode import settings
from geonode.services.serviceprocessors.base import get_geoserver_cascading_workspace
import os
from osgeo import ogr


class DataPublisher():
    def __init__(self) -> None:
        self.cat = Catalog(
            service_url=f"{settings.GEOSERVER_LOCATION}rest",
            username=settings.OGC_SERVER_DEFAULT_USER,
            password=settings.OGC_SERVER_DEFAULT_PASSWORD
        )
        self.workspace = get_geoserver_cascading_workspace(create=False)

    def _extract_resource_name_from_file(self, files):
        layers = ogr.Open(files.get("base_file"))
        return [_l.GetName() for _l in layers]


    def publish_resources(self, resources: List[str]):
        self.integrity_checks()
        for table_name in resources:
            try:
                self.cat.publish_featuretype(
                    name=table_name,
                    store=self.store, 
                    native_crs="EPSG:4326",
                    srs="EPSG:4326",
                    jdbc_virtual_table=table_name
                )
            except Exception as e:
                if f"Resource named '{table_name}' already exists in store:" in str(e):
                    continue
                raise e
    
    def integrity_checks(self):
        self.store = self.cat.get_store(
            name=os.getenv("GEONODE_GEODATABASE", "geonode_data"),
            workspace=self.workspace
        )
        if not self.store:
            raise Exception(f"The store does not exists: geonode_data")