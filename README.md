# geonode-importer
### C256-METAMEDIA-2022-GEOPACKAGE

installation: 
```
pip install -e git+https://github.com/geosolutions-it/geonode-importer.git@master#egg=geonode_importer
```

Add to settings:

```
CELERY_TASK_QUEUES += (
    Queue('importer.import_orchestrator', GEONODE_EXCHANGE, routing_key='importer.import_orchestrator', priority=0),
    Queue('importer.import_resource', GEONODE_EXCHANGE, routing_key='importer.import_resource', priority=0),
    Queue('importer.publish_resource', GEONODE_EXCHANGE, routing_key='importer.publish_resource', priority=0),
    Queue('importer.create_gn_resource', GEONODE_EXCHANGE, routing_key='importer.create_gn_resource', priority=0),
)

INSTALLED_APPS += ('importer', 'dynamic_models',)

DYNAMIC_MODELS = {
   "USE_APP_LABEL": "importer"
}

```

Run migrations:

```
python manage.py migrate
python manage.py migrate --database datastore
```
