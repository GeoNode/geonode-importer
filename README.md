# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/GeoNode/geonode-importer/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                                        |    Stmts |     Miss |   Cover |   Missing |
|-------------------------------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| importer/\_\_init\_\_.py                                                                    |        8 |        0 |    100% |           |
| importer/api/\_\_init\_\_.py                                                                |        0 |        0 |    100% |           |
| importer/api/exception.py                                                                   |       37 |        0 |    100% |           |
| importer/api/serializer.py                                                                  |       16 |        0 |    100% |           |
| importer/api/urls.py                                                                        |        5 |        0 |    100% |           |
| importer/api/views.py                                                                       |      124 |        3 |     98% |184-185, 273 |
| importer/apps.py                                                                            |       13 |        0 |    100% |           |
| importer/celery\_app.py                                                                     |        4 |        0 |    100% |           |
| importer/celery\_tasks.py                                                                   |      229 |       23 |     90% |55, 150, 246-249, 384-394, 555-565, 604, 650-660, 720-730, 778-783 |
| importer/datastore.py                                                                       |       20 |        1 |     95% |        34 |
| importer/db\_router.py                                                                      |       18 |        0 |    100% |           |
| importer/handlers/\_\_init\_\_.py                                                           |        0 |        0 |    100% |           |
| importer/handlers/apps.py                                                                   |       26 |        3 |     88% |     41-45 |
| importer/handlers/base.py                                                                   |      140 |       31 |     78% |43, 56, 61, 67, 107, 115, 125, 133, 199, 206, 228, 236, 245, 248, 256, 278-286, 295-296, 302, 330-340, 343, 348 |
| importer/handlers/common/\_\_init\_\_.py                                                    |        0 |        0 |    100% |           |
| importer/handlers/common/metadata.py                                                        |       42 |        4 |     90% |35, 44, 62, 88 |
| importer/handlers/common/raster.py                                                          |      224 |       76 |     66% |40, 44, 64, 73, 89, 106-107, 132-138, 145-147, 150-162, 165-176, 193, 210, 227-239, 303-305, 334, 395-406, 458-464, 478-487, 497, 504, 510-516, 534-569 |
| importer/handlers/common/remote.py                                                          |      107 |       14 |     87% |52, 74, 86-87, 151-153, 172, 273-284 |
| importer/handlers/common/serializer.py                                                      |       14 |        0 |    100% |           |
| importer/handlers/common/vector.py                                                          |      351 |       42 |     88% |55, 74, 83, 143-144, 211-212, 269, 275-278, 386-390, 452-456, 486, 590, 660-671, 795, 797-801, 810-815, 821-827, 867-868, 929-930, 945 |
| importer/handlers/csv/\_\_init\_\_.py                                                       |        0 |        0 |    100% |           |
| importer/handlers/csv/exceptions.py                                                         |        7 |        0 |    100% |           |
| importer/handlers/csv/handler.py                                                            |       94 |       21 |     78% |99, 158-223, 229, 242, 255 |
| importer/handlers/geojson/\_\_init\_\_.py                                                   |        0 |        0 |    100% |           |
| importer/handlers/geojson/exceptions.py                                                     |        7 |        0 |    100% |           |
| importer/handlers/geojson/handler.py                                                        |       56 |        3 |     95% | 76-77, 93 |
| importer/handlers/geotiff/\_\_init\_\_.py                                                   |        0 |        0 |    100% |           |
| importer/handlers/geotiff/exceptions.py                                                     |        7 |        0 |    100% |           |
| importer/handlers/geotiff/handler.py                                                        |       32 |        0 |    100% |           |
| importer/handlers/gpkg/\_\_init\_\_.py                                                      |        0 |        0 |    100% |           |
| importer/handlers/gpkg/exceptions.py                                                        |        7 |        0 |    100% |           |
| importer/handlers/gpkg/handler.py                                                           |       53 |        4 |     92% |58, 103, 114, 129 |
| importer/handlers/gpkg/tasks.py                                                             |        9 |        0 |    100% |           |
| importer/handlers/kml/\_\_init\_\_.py                                                       |        0 |        0 |    100% |           |
| importer/handlers/kml/exceptions.py                                                         |        7 |        0 |    100% |           |
| importer/handlers/kml/handler.py                                                            |       51 |        3 |     94% |58, 105, 114 |
| importer/handlers/remote/\_\_init\_\_.py                                                    |        0 |        0 |    100% |           |
| importer/handlers/remote/serializers/\_\_init\_\_.py                                        |        0 |        0 |    100% |           |
| importer/handlers/remote/serializers/wms.py                                                 |       10 |        0 |    100% |           |
| importer/handlers/remote/tests/\_\_init\_\_.py                                              |        0 |        0 |    100% |           |
| importer/handlers/remote/tiles3d.py                                                         |       51 |        6 |     88% |46, 52-53, 75, 78, 80 |
| importer/handlers/remote/wms.py                                                             |       63 |        3 |     95% |     82-86 |
| importer/handlers/shapefile/\_\_init\_\_.py                                                 |        0 |        0 |    100% |           |
| importer/handlers/shapefile/exceptions.py                                                   |        7 |        0 |    100% |           |
| importer/handlers/shapefile/handler.py                                                      |       87 |        6 |     93% |110, 138, 178, 188-190 |
| importer/handlers/shapefile/serializer.py                                                   |       19 |        0 |    100% |           |
| importer/handlers/sld/\_\_init\_\_.py                                                       |        0 |        0 |    100% |           |
| importer/handlers/sld/exceptions.py                                                         |        7 |        0 |    100% |           |
| importer/handlers/sld/handler.py                                                            |       29 |        2 |     93% |     72-75 |
| importer/handlers/tiles3d/\_\_init\_\_.py                                                   |        0 |        0 |    100% |           |
| importer/handlers/tiles3d/exceptions.py                                                     |        7 |        0 |    100% |           |
| importer/handlers/tiles3d/handler.py                                                        |      135 |       22 |     84% |143, 153-204, 228, 256-259, 302 |
| importer/handlers/tiles3d/utils.py                                                          |      107 |        2 |     98% |  140, 221 |
| importer/handlers/utils.py                                                                  |       58 |        8 |     86% |97-98, 111-112, 122-125 |
| importer/handlers/xml/\_\_init\_\_.py                                                       |        0 |        0 |    100% |           |
| importer/handlers/xml/exceptions.py                                                         |        7 |        0 |    100% |           |
| importer/handlers/xml/handler.py                                                            |       29 |        2 |     93% |     71-74 |
| importer/handlers/xml/serializer.py                                                         |       12 |        0 |    100% |           |
| importer/migrations/0001\_initial.py                                                        |        6 |        0 |    100% |           |
| importer/migrations/0002\_resourcehandlerinfo\_kwargs.py                                    |        4 |        0 |    100% |           |
| importer/migrations/0003\_resourcehandlerinfo\_execution\_id.py                             |        5 |        0 |    100% |           |
| importer/migrations/0004\_rename\_execution\_id\_resourcehandlerinfo\_execution\_request.py |        4 |        0 |    100% |           |
| importer/migrations/0005\_fixup\_dynamic\_shema\_table\_names.py                            |       19 |        7 |     63% |     19-25 |
| importer/migrations/0006\_dataset\_migration.py                                             |       21 |       12 |     43% |     14-29 |
| importer/migrations/\_\_init\_\_.py                                                         |        0 |        0 |    100% |           |
| importer/models.py                                                                          |       23 |        2 |     91% |     28-29 |
| importer/orchestrator.py                                                                    |      147 |       16 |     89% |69-70, 101, 158-160, 191, 259-268, 272-273, 284, 292-295 |
| importer/publisher.py                                                                       |       74 |       14 |     81% |61, 91, 94-111, 146-148, 193-194 |
| importer/settings.py                                                                        |        7 |        0 |    100% |           |
| importer/tests/\_\_init\_\_.py                                                              |        0 |        0 |    100% |           |
| importer/tests/end2end/\_\_init\_\_.py                                                      |        0 |        0 |    100% |           |
| importer/tests/unit/\_\_init\_\_.py                                                         |        0 |        0 |    100% |           |
| importer/tests/utils.py                                                                     |       20 |        0 |    100% |           |
| importer/urls.py                                                                            |        0 |        0 |    100% |           |
| importer/utils.py                                                                           |       27 |        2 |     93% |    24, 27 |
| importer/views.py                                                                           |        0 |        0 |    100% |           |
| setup.py                                                                                    |        7 |        7 |      0% |      1-15 |
|                                                                                   **TOTAL** | **2700** |  **339** | **87%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/GeoNode/geonode-importer/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/GeoNode/geonode-importer/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/GeoNode/geonode-importer/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/GeoNode/geonode-importer/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2FGeoNode%2Fgeonode-importer%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/GeoNode/geonode-importer/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.