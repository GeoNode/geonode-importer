# geonode-importer

A GeoNode 4.0 app that implements a brand new upload/import flow.  
The logic which adapts to different file types is modular.  
The implemented file type handlers so far are:
- GeoPackage
- GeoJSON
- KML


## System dependencies

### gdal-bin

The importer relies on the gdal utilities to perform format conversions and manipulations. 

You need to install the `gdal-bin` package in your system, be it a base system or a docker environment; in the latter case, make sure it is installed in the `celery` and in the `django` services.  
In a ubuntu/debian system you can install `gdal-bin` with the command:

    apt install gdal-bin

You need at least version `3.2.2` (this is the version that has been tested).  
To check your version please run either:

    $ gdalinfo --version
    GDAL 3.3.2, released 2021/09/01   

or

    $ ogrinfo --version
    GDAL 3.3.2, released 2021/09/01

To install `gdal-bin` on other platforms please refer to https://gdal.org/download.html.


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


### GeoJson

- Filename should not contain dots, for example "invalid.file.name.geojson" -> "valid_file_name.geojson"
