import json
import logging
import os
from pathlib import Path
import math

import requests
from geonode.layers.models import Dataset
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.upload.utils import UploadLimitValidator
from importer.handlers.common.remote import BaseRemoteResourceHandler
from importer.handlers.tiles3d.handler import Tiles3DFileHandler
from importer.handlers.tiles3d.utils import box_to_wgs84, sphere_to_wgs84
from importer.orchestrator import orchestrator
from importer.celery_tasks import import_orchestrator
from importer.handlers.common.vector import BaseVectorFileHandler
from importer.handlers.utils import create_alternate, should_be_imported
from importer.utils import ImporterRequestAction as ira
from geonode.base.models import ResourceBase
from importer.handlers.tiles3d.exceptions import Invalid3DTilesException

logger = logging.getLogger(__name__)


class RemoteTiles3DFileHandler(BaseRemoteResourceHandler, Tiles3DFileHandler):

    @staticmethod
    def can_handle(_data) -> bool:
        """
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        """
        if "url" in _data and '3dtiles' in _data.get("type"):
            return True
        return False


    @staticmethod
    def is_valid_url(url):
        BaseRemoteResourceHandler.is_valid_url(url)
        try:
            payload = requests.get(url, timeout=10).json()
            # required key described in the specification of 3dtiles
            # https://docs.ogc.org/cs/22-025r4/22-025r4.html#toc92
            is_valid = all(
                key in payload.keys() for key in ("asset", "geometricError", "root")
            )

            if not is_valid:
                raise Invalid3DTilesException(
                    "The provided 3DTiles is not valid, some of the mandatory keys are missing. Mandatory keys are: 'asset', 'geometricError', 'root'"
                )

            # if the keys are there, let's check if the mandatory child are there too
            asset = payload.get("asset", {}).get("version", None)
            if not asset:
                raise Invalid3DTilesException(
                    "The mandatory 'version' for the key 'asset' is missing"
                )
            volume = payload.get("root", {}).get("boundingVolume", None)
            if not volume:
                raise Invalid3DTilesException(
                    "The mandatory 'boundingVolume' for the key 'root' is missing"
                )

            error = payload.get("root", {}).get("geometricError", None)
            if error is None:
                raise Invalid3DTilesException(
                    "The mandatory 'geometricError' for the key 'root' is missing"
                )

        except Exception as e:
            raise Invalid3DTilesException(e)

        return True

    def create_geonode_resource(
        self,
        layer_name: str,
        alternate: str,
        execution_id: str,
        resource_type: Dataset = ...,
        asset=None,
    ):
        resource = super().create_geonode_resource(layer_name, alternate, execution_id, resource_type, asset)

        if self._has_region(js_file):
            resource = self.set_bbox_from_region(js_file, resource=resource)
        elif self._has_sphere(js_file):
            resource = self.set_bbox_from_boundingVolume_sphere(js_file, resource=resource)
        else:
            resource = self.set_bbox_from_boundingVolume(js_file, resource=resource)

        return resource

    def generate_resource_payload(self, layer_name, alternate, asset, _exec, workspace):
        return dict(
            resource_type="dataset",
            subtype="3dtiles",
            dirty_state=True,
            title=layer_name,
            owner=_exec.user,
            asset=asset,
            link_type="uploaded",
            extension="3dtiles",
            alternate=alternate,
        )

    def set_bbox_from_region(self, js_file, resource):
        # checking if the region is inside the json file
        region = js_file.get("root", {}).get("boundingVolume", {}).get("region", None)
        if not region:
            logger.info(
                f"No region found, the BBOX will not be updated for 3dtiles: {resource.title}"
            )
            return resource
        west, south, east, nord = region[:4]
        # [xmin, ymin, xmax, ymax]
        resource.set_bbox_polygon(
            bbox=[
                math.degrees(west),
                math.degrees(south),
                math.degrees(east),
                math.degrees(nord),
            ],
            srid="EPSG:4326",
        )

        return resource

    def set_bbox_from_boundingVolume(self, js_file, resource):
        transform_raw = js_file.get("root", {}).get("transform", [])
        box_raw = js_file.get("root", {}).get("boundingVolume", {}).get("box", None)

        if not box_raw or (not transform_raw and not box_raw):
            # skipping if values are missing from the json file
            return resource

        result = box_to_wgs84(box_raw, transform_raw)
        # [xmin, ymin, xmax, ymax]
        resource.set_bbox_polygon(
            bbox=[
                result["minx"],
                result["miny"],
                result["maxx"],
                result["maxy"],
            ],
            srid="EPSG:4326",
        )

        return resource
        
    def set_bbox_from_boundingVolume_sphere(self, js_file, resource):
        transform_raw = js_file.get("root", {}).get("transform", [])
        sphere_raw = js_file.get("root", {}).get("boundingVolume", {}).get("sphere", None)

        if not sphere_raw or (not transform_raw and not sphere_raw):
            # skipping if values are missing from the json file
            return resource
        if not transform_raw and (sphere_raw[0], sphere_raw[1], sphere_raw[2]) == (0, 0, 0):
            return resource
        result = sphere_to_wgs84(sphere_raw, transform_raw)
        # [xmin, ymin, xmax, ymax]
        resource.set_bbox_polygon(
            bbox=[
                result["minx"],
                result["miny"],
                result["maxx"],
                result["maxy"],
            ],
            srid="EPSG:4326",
        )

        return resource

    def _has_region(self, js_file):
        return js_file.get("root", {}).get("boundingVolume", {}).get("region", None)
    
    def _has_sphere(self, js_file):
        return js_file.get("root", {}).get("boundingVolume", {}).get("sphere", None)
