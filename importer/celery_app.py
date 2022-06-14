from celery import Celery

importer_app = Celery("importer")

# Using a string here means the worker will not have to
# pickle the object when using Windows.
importer_app.config_from_object('django.conf:settings', namespace="CELERY")