# geonode-importer
### C256-METAMEDIA-2022-GEOPACKAGE

installation: 
```
pip install -e git+https://github.com/geosolutions-it/geonode-importer.git@master#egg=geonode_importer
```

Add to settings:

```
INSTALLED_APPS += ('importer', 'dynamic_models',)

CELERY_TASK_QUEUES += (
    Queue('importer.import_orchestrator', GEONODE_EXCHANGE, routing_key='importer.import_orchestrator', priority=0),
    Queue('importer.import_resource', GEONODE_EXCHANGE, routing_key='importer.import_resource', priority=0),
    Queue('importer.publish_resource', GEONODE_EXCHANGE, routing_key='importer.publish_resource', priority=0),
    Queue('importer.create_gn_resource', GEONODE_EXCHANGE, routing_key='importer.create_gn_resource', priority=0),
    Queue('importer.gpkg_ogr2ogr', GEONODE_EXCHANGE, routing_key='importer.gpkg_ogr2ogr', priority=0),
    Queue('importer.gpkg_next_step', GEONODE_EXCHANGE, routing_key='importer.gpkg_next_step', priority=0),
    Queue('importer.gpkg_handler', GEONODE_EXCHANGE, routing_key='importer.gpkg_handler', priority=0),
)

DATABASE_ROUTERS = ["importer.db_router.DatastoreRouter"]

SIZE_RESTRICTED_FILE_UPLOAD_ELEGIBLE_URL_NAMES += ('importer_upload',)
```

Evironment Variables:

Is possible to define the rate limit for celery to handle the tasks by updating the following evironment variables:

```
IMPORTER_GLOBAL_RATE_LIMIT= # default 5
IMPORTER_PUBLISHING_RATE_LIMIT= # default 5
IMPORTER_RESOURCE_CREATION_RATE_LIMIT= # default 10
```


Run migrations:

```
python manage.py migrate
python manage.py migrate --database datastore
```
