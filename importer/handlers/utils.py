import hashlib

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


def create_alternate(layer_name, execution_id):
    '''
    Utility to generate the expected alternate for the resource
    is alternate = layer_name_ + md5(layer_name + uuid)
    '''
    _hash = hashlib.md5(
        f"{layer_name}_{execution_id}".encode('utf-8')
    ).hexdigest()
    alternate = f"{layer_name}_{_hash}"
    if len(alternate) >= 64: # 64 is the max table lengh in postgres
        return f"{layer_name[:50]}{_hash[:14]}"
    return alternate

