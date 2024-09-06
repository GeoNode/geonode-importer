import os

"""
main settings to handle the celery rate
"""
IMPORTER_GLOBAL_RATE_LIMIT = os.getenv("IMPORTER_GLOBAL_RATE_LIMIT", 5)
IMPORTER_PUBLISHING_RATE_LIMIT = os.getenv("IMPORTER_PUBLISHING_RATE_LIMIT", 5)
IMPORTER_RESOURCE_CREATION_RATE_LIMIT = os.getenv(
    "IMPORTER_RESOURCE_CREATION_RATE_LIMIT", 10
)
IMPORTER_RESOURCE_COPY_RATE_LIMIT = os.getenv("IMPORTER_RESOURCE_COPY_RATE_LIMIT", 10)

SYSTEM_HANDLERS = [
    'importer.handlers.gpkg.handler.GPKGFileHandler',
    'importer.handlers.geojson.handler.GeoJsonFileHandler',
    'importer.handlers.shapefile.handler.ShapeFileHandler',
    'importer.handlers.kml.handler.KMLFileHandler',
    'importer.handlers.csv.handler.CSVFileHandler',
    'importer.handlers.geotiff.handler.GeoTiffFileHandler',
    'importer.handlers.xml.handler.XMLFileHandler',
    'importer.handlers.sld.handler.SLDFileHandler',
    'importer.handlers.tiles3d.handler.Tiles3DFileHandler',
    'importer.handlers.remote.tiles3d.RemoteTiles3DResourceHandler',
    'importer.handlers.remote.wms.RemoteWMSResourceHandler',
]
