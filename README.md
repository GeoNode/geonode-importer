# geonode-importer

A GeoNode 4.0 app that implements a brand new upload/import flow.  
The logic which adapts to different file types is modular.  
The implemented file type handlers so far are:
- GeoPackage
- GeoJSON


## System dependencies

### GDAL
```
GDAL >= 3.2.2.1
```
To check your version please run: `gdalinfo --version`

The binaries for installing gdal are available [here](https://gdal.org/download.html)


## Installation

### Install the package

Make sure you activated the virtualenv if you are using one.
```bash
pip install -e git+https://github.com/geosolutions-it/geonode-importer.git@master#egg=geonode_importer
```

### Configuration

Add to your project's (or geonode's) `settings.py`:

```python
INSTALLED_APPS += ('dynamic_models', 'importer', 'importer.handlers',)

CELERY_TASK_QUEUES += (
    Queue('importer.import_orchestrator', GEONODE_EXCHANGE, routing_key='importer.import_orchestrator'),
    Queue('importer.import_resource', GEONODE_EXCHANGE, routing_key='importer.import_resource', max_priority=8),
    Queue('importer.publish_resource', GEONODE_EXCHANGE, routing_key='importer.publish_resource', max_priority=8),
    Queue('importer.create_geonode_resource', GEONODE_EXCHANGE, routing_key='importer.create_geonode_resource', max_priority=8),
    Queue('importer.import_with_ogr2ogr', GEONODE_EXCHANGE, routing_key='importer.import_with_ogr2ogr', max_priority=10),
    Queue('importer.import_next_step', GEONODE_EXCHANGE, routing_key='importer.import_next_step', max_priority=3),
    Queue('importer.create_dynamic_structure', GEONODE_EXCHANGE, routing_key='importer.create_dynamic_structure', max_priority=10),
    Queue('importer.copy_geonode_resource', GEONODE_EXCHANGE, routing_key='importer.copy_geonode_resource', max_priority=0),
    Queue('importer.copy_dynamic_model', GEONODE_EXCHANGE, routing_key='importer.copy_dynamic_model'),
    Queue('importer.copy_geonode_data_table', GEONODE_EXCHANGE, routing_key='importer.copy_geonode_data_table'),
)

DATABASE_ROUTERS = ["importer.db_router.DatastoreRouter"]

SIZE_RESTRICTED_FILE_UPLOAD_ELEGIBLE_URL_NAMES += ('importer_upload',)
```

### DB migration

Run migrations:

```bash
python manage.py migrate
python manage.py migrate --database datastore
```


## Available environment variables

To change the task rate limit, please update the following env variables:

```
IMPORTER_GLOBAL_RATE_LIMIT= # default 5
IMPORTER_PUBLISHING_RATE_LIMIT= # default 5
IMPORTER_RESOURCE_CREATION_RATE_LIMIT= # default 10
IMPORTER_RESOURCE_COPY_RATE_LIMIT = # default 10
```

## Supported file format

The importer will accept only:
- Vector GPKG
- Vector GeoJson


## Limitations

- The XML file and the SLD file uploaded along with the GPKG are ignored
- Every upload will create a new layer. There is no option for overwriting/skipping the existing layers
- The number of the layer in the GPKG should be lower than the `max_parallel_upload` configuration value


## Troubleshooting

### GeoPackage

The importer will return different error codes according to the encountered error.  
Here a description of the various codes:

| Code    |   Error                | Description |
|---------|------------------------|:------------|
| `RQ1`   | Invalid layer name     | Layer names must start with a letter, and valid characters are lowercase a-z, numbers, or underscores.|
| `RQ2`   | Empty layer            | Layers must have at least one feature.|
| `RQ13`  | SRS mismatch           | It is required to give all GEOMETRY features the same default spatial reference system|
| `RQ14`  | Unknown geometry type  | The geometry_type_name from the gpkg_geometry_columns table must be one of POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, or MULTIPOLYGON|
| `RQ15`  | Geometry type mismatch | All table geometries must match the geometry_type_name from the gpkg_geometry_columns table|
| `RC18`  | Geometry attr mismatch | It is recommended to give all GEOMETRY type columns the same name.|
