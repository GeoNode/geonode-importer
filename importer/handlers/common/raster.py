import pyproj
from importer.publisher import DataPublisher
from importer.utils import find_key_recursively
from itertools import chain
import json
import logging
from pathlib import Path
from subprocess import PIPE, Popen
from typing import List

from django.conf import settings
from django.db.models import Q
from django_celery_results.models import TaskResult
from geonode.base.models import ResourceBase
from geonode.layers.models import Dataset
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.resource.manager import resource_manager
from geonode.resource.models import ExecutionRequest
from geonode.services.serviceprocessors.base import \
    get_geoserver_cascading_workspace
from importer.api.exception import ImportException
from importer.celery_tasks import ErrorBaseTaskClass, import_orchestrator
from importer.handlers.base import BaseHandler
from importer.handlers.geotiff.exceptions import InvalidGeoTiffException
from importer.handlers.utils import create_alternate, should_be_imported
from importer.models import ResourceHandlerInfo
from importer.orchestrator import orchestrator
from osgeo import gdal
from importer.celery_app import importer_app
from geonode.storage.manager import storage_manager

logger = logging.getLogger(__name__)

gdal.UseExceptions()


class BaseRasterFileHandler(BaseHandler):
    """
    Handler to import Raster files into GeoNode data db
    It must provide the task_lists required to comple the upload
    """

    @property
    def default_geometry_column_name(self):
        return "geometry"

    @property
    def supported_file_extension_config(self):
        return NotImplementedError

    @staticmethod
    def is_valid(files, user):
        """
        Define basic validation steps
        """
        result = Popen("gdal_translate --version", stdout=PIPE, stderr=PIPE, shell=True)
        _, stderr = result.communicate()
        if stderr:
            raise ImportException(stderr)
        return True

    @staticmethod
    def can_handle(_data) -> bool:
        """
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        """
        return False

    @staticmethod
    def has_serializer(_data) -> bool:
        '''
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        '''
        return False

    @staticmethod
    def can_do(action) -> bool:
        """
        This endpoint will return True or False if with the info provided
        the handler is able to handle the file or not
        """
        return action in BaseHandler.ACTIONS

    @staticmethod
    def create_error_log(exc, task_name, *args):
        """
        This function will handle the creation of the log error for each message.
        This is helpful and needed, so each handler can specify the log as needed
        """
        return f"Task: {task_name} raised an error during actions for layer: {args[-1]}: {exc}"

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
            "skip_existing_layers": _data.pop("skip_existing_layers", "False"),
            "overwrite_existing_layer": _data.pop("overwrite_existing_layer", "False"),
            "store_spatial_file": _data.pop("store_spatial_files", "True"),
        }, _data

    @staticmethod
    def publish_resources(resources: List[str], catalog, store, workspace):
        """
        Given a list of strings (which rappresent the table on geoserver)
        Will publish the resorces on geoserver
        """
        for _resource in resources:
            try:
                catalog.create_coveragestore(
                    _resource.get("name"),
                    path=_resource.get("raster_path"),
                    layer_name=_resource.get("name"),
                    workspace=workspace,
                    overwrite=True,
                    upload_data=False
                )
            except Exception as e:
                if (
                    f"Resource named {_resource.get('name')} already exists in store:"
                    in str(e)
                ):
                    continue
                raise e
        return True

    @staticmethod
    def delete_resource(instance):
        # it should delete the image from the geoserver data dir
        # for now we can rely on the geonode delete behaviour
        # since the file is stored on local
        pass

    @staticmethod
    def perform_last_step(execution_id):
        '''
        Override this method if there is some extra step to perform
        before considering the execution as completed.
        For example can be used to trigger an email-send to notify
        that the execution is completed
        '''
        # as last step, we delete the celery task to keep the number of rows under control
        lower_exec_id = execution_id.replace("-", "_").lower()
        TaskResult.objects.filter(
            Q(task_args__icontains=lower_exec_id)
            | Q(task_kwargs__icontains=lower_exec_id)
            | Q(result__icontains=lower_exec_id)
            | Q(task_args__icontains=execution_id)
            | Q(task_kwargs__icontains=execution_id)
            | Q(result__icontains=execution_id)
        ).delete()

        _exec = orchestrator.get_execution_object(execution_id)

        _exec.save()

        _exec.output_params.update(**{
            "detail_url": [
                x.resource.detail_url
                for x in ResourceHandlerInfo.objects.filter(execution_request=_exec)
            ]
        })


    def extract_resource_to_publish(self, files, action, layer_name, alternate, **kwargs):
        if action == exa.COPY.value:
            return [
                {
                    "name": alternate,
                    "crs": ResourceBase.objects.filter(Q(alternate__icontains=layer_name) | Q(title__icontains=layer_name))
                    .first()
                    .srid,
                    "raster_path": kwargs['kwargs'].get("new_file_location").get("files")[0]
                }
            ]

        layers = gdal.Open(files.get("base_file"))
        if not layers:
            return []
        return [{
                "name": alternate or layer_name,
                "crs": self.identify_authority(layers) if layers.GetSpatialRef() else None,
                "raster_path": files.get("base_file")
            }]

    def identify_authority(self, layer):
        try:
            layer_wkt = layer.GetSpatialRef().ExportToWkt()
            x = pyproj.CRS(layer_wkt)
            _name = "EPSG"
            _code = x.to_epsg(min_confidence=20)
            if _code is None:
                raise Exception("authority code not found, fallback to default behaviour")
        except:
            spatial_ref = layer.GetSpatialRef()
            spatial_ref.AutoIdentifyEPSG()
            _name = spatial_ref.GetAuthorityName(None) or spatial_ref.GetAttrValue('AUTHORITY', 0)
            _code = spatial_ref.GetAuthorityCode('PROJCS') or spatial_ref.GetAuthorityCode('GEOGCS') or spatial_ref.GetAttrValue('AUTHORITY', 1)
        return f"{_name}:{_code}"

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
        orchestrator.update_execution_request_status(execution_id=str(execution_id), input_params=_input)

        try:
            filename = Path(files.get("base_file")).stem
            # start looping on the layers available
            layer_name = self.fixup_name(filename)

            should_be_overwritten = _exec.input_params.get("overwrite_existing_layer")
            # should_be_imported check if the user+layername already exists or not
            if (
                should_be_imported(
                    layer_name,
                    _exec.user,
                    skip_existing_layer=_exec.input_params.get(
                        "skip_existing_layer"
                    ),
                    overwrite_existing_layer=should_be_overwritten,
                )
            ):
                workspace = get_geoserver_cascading_workspace(create=False)
                user_datasets = Dataset.objects.filter(
                    owner=_exec.user, alternate=f"{workspace.name}:{layer_name}"
                )

                dataset_exists = user_datasets.exists()

                if dataset_exists and should_be_overwritten:
                    layer_name, alternate = layer_name, user_datasets.first().alternate
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
        return

    def create_geonode_resource(
        self, layer_name: str, alternate: str, execution_id: str, resource_type: Dataset = Dataset, files=None
    ):
        """
        Base function to create the resource into geonode. Each handler can specify
        and handle the resource in a different way
        """
        saved_dataset = resource_type.objects.filter(alternate__icontains=alternate)

        _exec = self._get_execution_request_object(execution_id)

        workspace = getattr(
            settings,
            "DEFAULT_WORKSPACE",
            getattr(settings, "CASCADE_WORKSPACE", "geonode"),
        )

        _overwrite = _exec.input_params.get("overwrite_existing_layer", False)
        # if the layer exists, we just update the information of the dataset by
        # let it recreate the catalogue
        if not saved_dataset.exists() and _overwrite:
            logger.warning(
                f"The dataset required {alternate} does not exists, but an overwrite is required, the resource will be created"
            )
        saved_dataset = resource_manager.create(
            None,
            resource_type=resource_type,
            defaults=dict(
                name=alternate,
                workspace=workspace,
                subtype="raster",
                alternate=f"{workspace}:{alternate}",
                dirty_state=True,
                title=layer_name,
                owner=_exec.user,
                files=list(set(list(_exec.input_params.get("files", {}).values()) or list(files))),
            ),
        )

        saved_dataset.refresh_from_db()

        self.handle_xml_file(saved_dataset, _exec)
        self.handle_sld_file(saved_dataset, _exec)

        resource_manager.set_thumbnail(None, instance=saved_dataset)

        ResourceBase.objects.filter(alternate=alternate).update(dirty_state=False)

        saved_dataset.refresh_from_db()
        return saved_dataset

    def overwrite_geonode_resource(
        self, layer_name: str, alternate: str, execution_id: str, resource_type: Dataset = Dataset, files=None
    ):

        dataset = resource_type.objects.filter(alternate__icontains=alternate)

        _exec = self._get_execution_request_object(execution_id)

        _overwrite = _exec.input_params.get("overwrite_existing_layer", False)
        # if the layer exists, we just update the information of the dataset by
        # let it recreate the catalogue
        if dataset.exists() and _overwrite:
            dataset = dataset.first()

            dataset = resource_manager.update(dataset.uuid, instance=dataset)

            self.handle_xml_file(dataset, _exec)
            self.handle_sld_file(dataset, _exec)

            resource_manager.set_thumbnail(self.object.uuid, instance=self.object, overwrite=False)
            dataset.refresh_from_db()
            return dataset
        elif not dataset.exists() and _overwrite:
            logger.warning(
                f"The dataset required {alternate} does not exists, but an overwrite is required, the resource will be created"
            )
            return self.create_geonode_resource(layer_name, alternate, execution_id, resource_type, files)
        elif not dataset.exists() and not _overwrite:
            logger.warning(
                "The resource does not exists, please use 'create_geonode_resource' to create one"
            )
        return

    def handle_xml_file(self, saved_dataset: Dataset, _exec: ExecutionRequest):
        _path = _exec.input_params.get("files", {}).get("xml_file", "")
        resource_manager.update(
            None,
            instance=saved_dataset,
            xml_file=_path,
            metadata_uploaded=True if _path else False,
            vals={"dirty_state": True},
        )

    def handle_sld_file(self, saved_dataset: Dataset, _exec: ExecutionRequest):
        _path = _exec.input_params.get("files", {}).get("sld_file", "")
        resource_manager.exec(
            "set_style",
            None,
            instance=saved_dataset,
            sld_file=_exec.input_params.get("files", {}).get("sld_file", ""),
            sld_uploaded=True if _path else False,
            vals={"dirty_state": True},
        )

    def create_resourcehandlerinfo(self, handler_module_path: str, resource: Dataset, execution_id: ExecutionRequest, **kwargs):
        """
        Create relation between the GeonodeResource and the handler used
        to create/copy it
        """
        ResourceHandlerInfo.objects.create(
            handler_module_path=str(handler_module_path),
            resource=resource,
            execution_request=execution_id,
            kwargs=kwargs.get('kwargs', {})
        )

    def overwrite_resourcehandlerinfo(self, handler_module_path: str, resource: Dataset, execution_id: ExecutionRequest, **kwargs):
        """
        Overwrite the ResourceHandlerInfo
        """
        if resource.resourcehandlerinfo_set.exists():
            resource.resourcehandlerinfo_set.update(
                handler_module_path=handler_module_path,
                resource=resource,
                execution_request=execution_id,
                kwargs=kwargs.get('kwargs', {}) or kwargs
            )
            return
        return self.create_resourcehandlerinfo(handler_module_path, resource, execution_id, **kwargs)

    def copy_geonode_resource(
        self, alternate: str, resource: Dataset, _exec: ExecutionRequest, data_to_update: dict, new_alternate: str, **kwargs
    ):
        resource = self.create_geonode_resource(
            layer_name=data_to_update.get("title"),
            alternate=new_alternate,
            execution_id=str(_exec.exec_id),
            files=kwargs.get("kwargs", {}).get("new_file_location", {}).get("files", [])
        )
        resource.refresh_from_db()
        return resource

    def _get_execution_request_object(self, execution_id: str):
        return ExecutionRequest.objects.filter(exec_id=execution_id).first()

    @staticmethod
    def copy_original_file(dataset):
        '''
        Copy the original file into a new location
        '''
        return storage_manager.copy(dataset)

    def rollback(self, exec_id, rollback_from_step, action_to_rollback, *args, **kwargs):
        steps = self.ACTIONS.get(action_to_rollback)
        step_index = steps.index(rollback_from_step)
        # the start_import, start_copy etc.. dont do anything as step, is just the start
        # so there is nothing to rollback
        steps_to_rollback = steps[1:step_index+1]
        if not steps_to_rollback:
            return
        # reversing the tuple to going backwards with the rollback
        reversed_steps = steps_to_rollback[::-1]
        istance_name = None
        try:
            istance_name = find_key_recursively(kwargs, "new_dataset_alternate") or args[3]
        except:
            pass
        
        logger.warning(f"Starting rollback for execid: {exec_id} resource published was: {istance_name}")

        for step in reversed_steps:
            normalized_step_name = step.split(".")[-1]
            if getattr(self, f"_{normalized_step_name}_rollback", None):
                function = getattr(self, f"_{normalized_step_name}_rollback")
                function(exec_id, istance_name, *args, **kwargs)

        logger.warning(f"Rollback for execid: {exec_id} resource published was: {istance_name} completed")

    def _import_resource_rollback(self, exec_id, istance_name=None, *args, **kwargs):
        '''
        In the raster, this step just generate the alternate, no real action
        are done on the database
        '''
        pass

    def _publish_resource_rollback(self, exec_id, istance_name=None, *args, **kwargs):       
        '''
        We delete the resource from geoserver
        '''
        logger.info(f"Rollback publishing step in progress for execid: {exec_id} resource published was: {istance_name}")
        exec_object = orchestrator.get_execution_object(exec_id)
        handler_module_path = exec_object.input_params.get("handler_module_path")
        publisher = DataPublisher(handler_module_path=handler_module_path)
        publisher.delete_resource(istance_name)
    
    def _create_geonode_resource_rollback(self, exec_id, istance_name=None, *args, **kwargs):
        '''
        The handler will remove the resource from geonode
        '''
        logger.info(f"Rollback geonode step in progress for execid: {exec_id} resource created was: {istance_name}")
        resource = ResourceBase.objects.filter(alternate__icontains=istance_name)
        if resource.exists():
            resource.delete()
    
    def _copy_dynamic_model_rollback(self, exec_id, istance_name=None, *args, **kwargs):
        self._import_resource_rollback(exec_id, istance_name=istance_name)
    
    def _copy_geonode_resource_rollback(self, exec_id, istance_name=None, *args, **kwargs):
        self._create_geonode_resource_rollback(exec_id, istance_name=istance_name)


@importer_app.task(
    base=ErrorBaseTaskClass,
    name="importer.copy_raster_file",
    queue="importer.copy_raster_file",
    max_retries=1,
    acks_late=False,
    ignore_result=False,
    task_track_started=True,
)
def copy_raster_file(
    exec_id,
    actual_step,
    layer_name,
    alternate,
    handler_module_path,
    action,
    **kwargs
):
    """
    Perform a copy of the original raster file    """

    original_dataset = ResourceBase.objects.filter(alternate=alternate)
    if not original_dataset.exists():
        raise InvalidGeoTiffException("Dataset required does not exists")

    original_dataset = original_dataset.first()

    if not original_dataset.files:
        raise InvalidGeoTiffException("The original file of the dataset is not available, Is not possible to copy the dataset")

    new_file_location = orchestrator.load_handler(handler_module_path).copy_original_file(original_dataset)

    new_dataset_alternate = create_alternate(original_dataset.title, exec_id)

    additional_kwargs = {
        "original_dataset_alternate": original_dataset.alternate,
        "new_dataset_alternate": new_dataset_alternate,
        "new_file_location": new_file_location
    }

    task_params = (
        {},
        exec_id,
        handler_module_path,
        actual_step,
        layer_name,
        new_dataset_alternate,
        action,
    )

    import_orchestrator.apply_async(task_params, additional_kwargs)

    return "copy_raster", layer_name, alternate, exec_id
