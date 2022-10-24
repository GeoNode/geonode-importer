from django.apps import AppConfig


class ImporterConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "importer"

    def ready(self):
        """Finalize setup"""
        run_setup_hooks()
        super(ImporterConfig, self).ready()


def run_setup_hooks(*args, **kwargs):
    """
    Run basic setup configuration for the importer app.
    Here we are overriding the upload API url
    """
    from django.conf.urls import include, url
    from geonode.urls import urlpatterns

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
