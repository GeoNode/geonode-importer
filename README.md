# geonode-importer
### C256-METAMEDIA-2022-GEOPACKAGE

installation: 
```
pip install -e git+https://github.com/geosolutions-it/geonode-importer.git@master#egg=geonode_importer
```

Add to settings:

```
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
