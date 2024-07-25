import json
import logging
import os

import requests
from geonode.layers.models import Dataset
from geonode.resource.enumerator import ExecutionRequestAction as exa
from importer.api.exception import ImportException
from importer.handlers.base import BaseHandler
from importer.handlers.common.serializer import RemoteResourceSerializer
from importer.models import ResourceHandlerInfo
from importer.orchestrator import orchestrator
from importer.celery_tasks import import_orchestrator
from importer.handlers.utils import create_alternate
from importer.utils import ImporterRequestAction as ira
from geonode.base.models import ResourceBase, Link
from urllib.parse import urlparse
from geonode.base.enumerations import SOURCE_TYPE_REMOTE
from geonode.resource.manager import resource_manager
from geonode.resource.models import ExecutionRequest

logger = logging.getLogger(__name__)


class BaseRemoteResourceHandler(BaseHandler):
    """
    Handler to import remote resources into GeoNode data db
    It must provide the task_lists required to comple the upload
    As first implementation only remote 3dtiles are supported
    """

    ACTIONS = {
        exa.IMPORT.value: (
            "start_import",
            "importer.import_resource",
            "importer.create_geonode_resource",
        ),
        exa.COPY.value: (
            "start_copy",
            "importer.copy_geonode_resource",
        ),
        ira.ROLLBACK.value: (
            "start_rollback",
            "importer.rollback",
        ),
    }

    @staticmethod
    def has_serializer(data) -> bool:
        if "url" in data:
            return RemoteResourceSerializer
        return False

    @staticmethod
    def can_handle(_data) -> bool:
        """
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        """
        if "url" in _data:
            return True
        return False

    @staticmethod
    def is_valid_url(url):
        """
        We mark it as valid if the urls is reachable
        and if the url is valid
        """
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
        except requests.exceptions.Timeout:
            raise ImportException("Timed out")
        except Exception:
            raise ImportException("The provided URL is not reachable")
        return True

    @staticmethod
    def extract_params_from_data(_data, action=None):
        """
        Remove from the _data the params that needs to save into the executionRequest object
        all the other are returned
        """
        if action == exa.COPY.value:
            title = json.loads(_data.get("defaults"))
            return {"title": title.pop("title")}, _data

        return {
            "source": _data.pop("source", "upload"),
            "title": _data.pop("title", None),
            "url": _data.pop("url", None),
            "type": _data.pop("type", None),
        }, _data

    def import_resource(self, files: dict, execution_id: str, **kwargs) -> str:
        """
        Main function to import the resource.
        Internally will call the steps required to import the
        data inside the geonode_data database
        """
        # for the moment we skip the dyanamic model creation
        logger.info("Total number of layers available: 1")
        _exec = self._get_execution_request_object(execution_id)
        _input = {**_exec.input_params, **{"total_layers": 1}}
        orchestrator.update_execution_request_status(
            execution_id=str(execution_id), input_params=_input
        )

        try:
            params = _exec.input_params.copy()
            url = params.get("url")
            title = params.get("title", os.path.basename(urlparse(url).path))

            # start looping on the layers available
            layer_name = self.fixup_name(title)

            should_be_overwritten = _exec.input_params.get("overwrite_existing_layer")

            user_datasets = ResourceBase.objects.filter(
                owner=_exec.user, alternate=layer_name
            )

            dataset_exists = user_datasets.exists()

            if dataset_exists and should_be_overwritten:
                layer_name, alternate = (
                    layer_name,
                    user_datasets.first().alternate.split(":")[-1],
                )
            elif not dataset_exists:
                alternate = layer_name
            else:
                alternate = create_alternate(layer_name, execution_id)

            import_orchestrator.apply_async(
                (
                    files,
                    execution_id,
                    str(self),
                    "importer.import_resource",
                    layer_name,
                    alternate,
                    exa.IMPORT.value,
                )
            )
            return layer_name, alternate, execution_id

        except Exception as e:
            logger.error(e)
            raise e

    def create_geonode_resource(
        self,
        layer_name: str,
        alternate: str,
        execution_id: str,
        resource_type: Dataset = ...,
        asset=None,
    ):
        """
        Creating geonode base resource
        We ignore the params, we use the function as a interface to keep the same
        importer flow.
        We create a standard ResourceBase
        """
        _exec = orchestrator.get_execution_object(execution_id)
        params = _exec.input_params.copy()
        subtype = params.get("type")

        resource = resource_manager.create(
            None,
            resource_type=ResourceBase,
            defaults=dict(
                resource_type="dataset",
                subtype=subtype,
                sourcetype=SOURCE_TYPE_REMOTE,
                alternate=alternate,
                dirty_state=True,
                title=params.get("title", layer_name),
                owner=_exec.user,
            ),
        )
        resource_manager.set_thumbnail(None, instance=resource)

        resource = self.create_link(resource, params, alternate)
        ResourceBase.objects.filter(alternate=alternate).update(dirty_state=False)

        return resource

    def create_link(self, resource, params: dict, name):
        link = Link(
            resource=resource,
            extension=params.get("type"),
            url=params.get("url"),
            link_type="data",
            name=name,
        )
        link.save()
        return resource

    def create_resourcehandlerinfo(
        self,
        handler_module_path: str,
        resource: Dataset,
        execution_id: ExecutionRequest,
        **kwargs,
    ):
        """
        Create relation between the GeonodeResource and the handler used
        to create/copy it
        """

        ResourceHandlerInfo.objects.create(
            handler_module_path=handler_module_path,
            resource=resource,
            execution_request=execution_id,
            kwargs=kwargs.get("kwargs", {}) or kwargs,
        )
