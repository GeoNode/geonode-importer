from django.apps import AppConfig


class ImporterConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "importer"

    def ready(self):
        """Finalize setup"""
        run_setup_hooks()
        super(ImporterConfig, self).ready()


def run_setup_hooks(*args, **kwargs):
    '''
    Run basic setup configuration for the importer app.
    Here we are overriding the upload API url
    '''
    from django.conf.urls import include, url
    from geonode.urls import urlpatterns
    from django.conf import settings

    url_already_injected = any(
        [
            "importer.urls" in x.urlconf_name.__name__
            for x in urlpatterns
            if hasattr(x, "urlconf_name") and not isinstance(x.urlconf_name, list)
        ]
    )

    if not url_already_injected:
        urlpatterns.insert(
            0,
            url(r"^api/v2/", include("importer.api.urls")),
        )

    # injecting the new config required for FE
    gpkg_config = [{"id": 'gpkg', "label": 'GeoPackage', "format": 'archive', "ext": ['gpkg']}]
    if not getattr(settings, 'ADDITIONAL_DATASET_FILE_TYPES', None):
        setattr(settings, 'ADDITIONAL_DATASET_FILE_TYPES', gpkg_config)
    elif 'gpkg' not in [x.get('id') for x in settings.ADDITIONAL_DATASET_FILE_TYPES]:
        settings.ADDITIONAL_DATASET_FILE_TYPES.extend(gpkg_config)
        setattr(settings, "ADDITIONAL_DATASET_FILE_TYPES", settings.ADDITIONAL_DATASET_FILE_TYPES)

    additional_router = ["importer.db_router.DatastoreRouter"]
    if not getattr(settings, 'DATABASE_ROUTERS', None):
        setattr(settings, 'DATABASE_ROUTERS', additional_router)
    elif 'importer.db_router.DatastoreRouter' not in settings.DATABASE_ROUTERS:
        settings.DATABASE_ROUTERS.extend(additional_router)
        setattr(settings, "DATABASE_ROUTERS", settings.DATABASE_ROUTERS)

    settings.SIZE_RESTRICTED_FILE_UPLOAD_ELEGIBLE_URL_NAMES += ('importer_upload',)
