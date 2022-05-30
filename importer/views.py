from celery import Celery
from geonode.tasks.tasks import FaultTolerantTask


app = Celery('importer')

@app.task(
    bind=True,
    base=FaultTolerantTask,
    name='importer.run_dataset_import',
    queue='geonode.dataset_importer',
    expires=600,
    time_limit=600,
    acks_late=False,
    autoretry_for=(Exception, ),
    retry_kwargs={'max_retries': 3},
    retry_backoff=3,
    retry_backoff_max=30,
    retry_jitter=False
)
def run_dataset_import(data, store_spatial_files):
    print(data, store_spatial_files)
    pass