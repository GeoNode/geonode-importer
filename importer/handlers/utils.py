from django.contrib.auth import get_user_model
from geonode.base.models import ResourceBase
from geonode.services.serviceprocessors.base import \
    get_geoserver_cascading_workspace

def should_be_imported(layer: str, user: get_user_model(), **kwargs) -> bool:
    '''
    If layer_name + user (without the addition of any execution id)
    already exists, will apply one of the rule available:
    - skip_existing_layer: means that if already exists will be skept
    - override_layer: means that if already exists will be overridden
        - the dynamic model should be recreated
        - ogr2ogr should override the layer
        - the publisher should republish the resource
        - geonode should update it
    '''
    workspace = get_geoserver_cascading_workspace(create=False)
    exists = ResourceBase.objects.filter(
        alternate=f"{workspace.name}:{layer}",
        owner=user
    ).exists()

    if exists and kwargs.get("skip_existing_layer", False):
        return False
    
    return True
