from abc import ABC
from geonode.base.models import ResourceBase
from importer.publisher import DataPublisher
import logging
from typing import List

from dynamic_models.models import ModelSchema
from dynamic_models.schema import ModelSchemaEditor
from geonode.resource.enumerator import ExecutionRequestAction as exa
from geonode.layers.models import Dataset
from importer.utils import ImporterRequestAction as ira, find_key_recursively

logger = logging.getLogger(__name__)


class BaseHandler(ABC):
    """
    Base abstract handler object
    define the required method needed to define an upload handler
    it must be:
    - provide the tasks list to complete the import
    - validation function
    - method to import the resource
    - create_error_log
    """

    REGISTRY = []

    ACTIONS = {
        exa.IMPORT.value: (),
        exa.COPY.value: (),
        exa.DELETE.value: (),
        exa.UPDATE.value: (),
        ira.ROLLBACK.value: (),
    }

    def __str__(self):
        return f"{self.__module__}.{self.__class__.__name__}"

    def __repr__(self):
        return self.__str__()

    @classmethod
    def register(cls):
        BaseHandler.REGISTRY.append(cls)

    @classmethod
    def get_registry(cls):
        return BaseHandler.REGISTRY

    @classmethod
    def get_task_list(cls, action) -> tuple:
        if action not in cls.ACTIONS:
            raise Exception("The requested action is not implemented yet")
        return cls.ACTIONS.get(action)

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
        return NotImplementedError

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
        This endpoint should return (if set) the custom serializer used in the API
        to validate the input resource
        '''
        return None

    @staticmethod
    def can_do(action) -> bool:
        """
        Evaluate if the handler can take care of a specific action.
        Each action (import/copy/etc...) can define different step so
        the Handler must be ready to handle them. If is not in the actual
        flow the already in place flow is followd
        """
        return action in BaseHandler.ACTIONS

    @staticmethod
    def extract_params_from_data(_data):
        """
        Remove from the _data the params that needs to save into the executionRequest object
        all the other are returned
        """
        return []

    def fixup_name(self, name):
        return name.lower().replace("-", "_")\
            .replace(" ", "_").replace(")", "")\
            .replace("(", "").replace(",", "")\
            .replace("&", "").replace(".", "")

    def extract_resource_to_publish(self, files, layer_name, alternate, **kwargs):
        """
        Function to extract the layer name and the CRS from needed in the
        publishing phase
        [
            {'name': 'alternate or layer_name', 'crs': 'EPSG:25832'}
        ]
        """
        return NotImplementedError

    @staticmethod
    def create_error_log(exc, task_name, *args):
        """
        This function will handle the creation of the log error for each message.
        This is helpful and needed, so each handler can specify the log as needed
        """
        return f"Task: {task_name} raised an error during actions for layer: {args[-1]}: {exc}"

    def import_resource(self, files: dict, execution_id: str, **kwargs):
        """
        Define the step to perform the import of the data
        into the datastore db
        """
        return NotImplementedError

    @staticmethod
    def publish_resources(resources: List[str], catalog, store, workspace):
        """
        Given a list of strings (which rappresent the table on geoserver)
        Will publish the resorces on geoserver
        """
        return NotImplementedError

    def create_geonode_resource(
        self, layer_name, alternate, execution_id, resource_type: Dataset = Dataset
    ):
        """
        Base function to create the resource into geonode. Each handler can specify
        and handle the resource in a different way
        """
        return NotImplementedError

    def create_resourcehandlerinfo(self, handler_module_path, resource, **kwargs):
        return NotImplementedError

    def get_ogr2ogr_task_group(
        self, execution_id, files, layer, should_be_overwritten, alternate
    ):
        """
        implement custom ogr2ogr task group
        """
        return NotImplementedError

    def delete_resource(self, instance):
        """
        Base function to delete the resource with all the dependencies (example: dynamic model)
        """
        return NotImplementedError

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
        We use the schema editor directly, because the model itself is not managed
        on creation, but for the delete since we are going to handle, we can use it
        '''
        logger.info(f"Rollback dynamic model step in progress for execid: {exec_id} resource published was: {istance_name}")
        schema = ModelSchema.objects.filter(name=istance_name).first()
        if schema is not None:
            _model_editor = ModelSchemaEditor(initial_model=istance_name, db_name=schema.db_name)
            _model_editor.drop_table(schema.as_model())
            ModelSchema.objects.filter(name=istance_name).delete()

    def _publish_resource_rollback(self, exec_id, istance_name=None, *args, **kwargs):
        from importer.orchestrator import orchestrator
        
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
