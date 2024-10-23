![PyPI - Downloads](https://img.shields.io/pypi/dm/geonode-importer) ![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/GeoNode/geonode-importer/runtests.yml) ![GitHub top language](https://img.shields.io/github/languages/top/GeoNode/geonode-importer)

# NOTE:
GeoNode 5 (master branch) includes the importer in its core. This repository and the `geonode-importer` package will be maintained for GeoNode <= 4.4.x

For more information https://github.com/GeoNode/geonode/issues/12368.

# geonode-importer

A GeoNode app that implements a flow to upload/import data files.  
The modular logic adapts to different file types, and can be extended by implementing custom handlers.  

## Some history
`geonode-importer` has been created as a GeoNode 4.0 app to handle some formats that at the time were unsupported by GeoNode.
It has then been extended to include all previously handled formats.  
In GeoNode 4.1 `geonode-importer` replaced the previous importer logic.

## Supported file formats
- **ESRI Shapefile** - Vector
- **GeoPackage** - Vector
- **GeoJSON** - Vector
- **KML** - Vector
- **CSV** - Vector
- **GeoTiff** - Raster
- **XML** - Update XML file for a given resource
- **SLD** - Update SLD file for a given resource

**IMPORTANT**: At the moment the importer doesn't support overwriting/skipping existing layers from the UI. Every upload will create a new dataset.
Overwriting a layer (`overwrite_existing_layer`) and skipping an already existing layer (`skip_existing_layers`) is supported through the API. 
Refer to the [API documentation](http://localhost:5500/_build/html/en/devel/api/usage/index.html#resource-upload) for more details and exmplaes.

### GeoPackage
- Features in the same table must have the same geometry type. Mixed geometry tpyes are not supported, therefore `GEOMETRY` columns are not accepted
- The XML file and the SLD file uploaded along with the GPKG are ignored
- The number of layers in a GPKG must be lower than the `max_parallel_upload` configuration value
### GeoJSON
- The filename should not contain dots, for example "invalid.file.name.geojson" -> "valid_file_name.geojson"

### CSV
- The CSV colum accepted for lat/long CSVs (`POINTS`) are the followings:
  - `lat`, `latitude`, `y`
  - `long`, `longitude`, `x`
- For any other geometry type the following columns are accepted:
  - `geom`, `geometry`, `the_geom`, `wkt_geom`


## Installation
**Starting from GeoNode 4.1.0 the new importer is installed and configured by default**. 

The following documentation is only meant to report what is automatically done under the hood.
### System dependencies

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

### Install the package

Make sure you activated the virtualenv if you are using one.
```bash
pip install -e git+https://github.com/geosolutions-it/geonode-importer.git@master#egg=geonode_importer
```

### Configuration

The following settings in GeoNode's `settings.py` drive the importer functionality:

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
    Queue('importer.copy_raster_file', GEONODE_EXCHANGE, routing_key='importer.copy_raster_file'),
    Queue('importer.rollback', GEONODE_EXCHANGE, routing_key='importer.rollback'),

)

DATABASE_ROUTERS = ["importer.db_router.DatastoreRouter"]

SIZE_RESTRICTED_FILE_UPLOAD_ELEGIBLE_URL_NAMES += ('importer_upload',)

IMPORTER_HANDLERS = os.getenv('IMPORTER_HANDLERS', [
    'importer.handlers.gpkg.handler.GPKGFileHandler',
    'importer.handlers.geojson.handler.GeoJsonFileHandler',
    'importer.handlers.shapefile.handler.ShapeFileHandler',
    'importer.handlers.kml.handler.KMLFileHandler',
    'importer.handlers.csv.handler.CSVFileHandler',
    'importer.handlers.geotiff.handler.GeoTiffFileHandler',
    'importer.handlers.xml.handler.XMLFileHandler',
    'importer.handlers.sld.handler.SLDFileHandler'
])

```

## NOTE:
In case of a local environment, Geoserver and Geonode should be able to reach the default `MEDIA_ROOT`.

If some permission is missing, please change the `FILE_UPLOAD_DIRECTORY_PERMISSIONS` to make the folder accessible to both

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

# https://github.com/OSGeo/gdal/issues/8674
OGR2OGR_COPY_WITH_DUMP = If true, will pipe the PG dump to psql.
```

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

## Character conversion

By default, we use the `LAUNDER` option in ogr2ogr to convert a list of special chars to make them compatible with PostgreSQL.

Along with this, also the layer name during the dataset import is converted to make it aligned with ogr2ogr.

In detail the list is the following:

| From | TO  |
|---|---|
| - (space) # \  | _  |
| .)(,&  | empty_string  |


## Run tests

A simple docker compose is provided along with the project. To run the geonode-importer test suite please follow this steps:

The compose project, provides what is needed to run the tests. This includes:

1) Database
1) Geoserver
1) Django with all the dependecies needed


#### Step 1: build the project

Build and run the docker compose project
```
docker compose -f docker-compose-test.yaml up --build -d
```

#### Step 2: Run Scripts

Two scripts are provided along with the compose file:

1) entrypoint_test.sh
1) runtest.sh

#### 1) entrypoint_test.sh
Is a script which create the needed extension and database to run the tests. Is a setup script

#### 2) runtest.sh
It actually runs the tests.


Note: the tests inside the docker environment can be run manually too

## Env File
The `.env_test` file contains all the environment variable needed to successfully run the tests
