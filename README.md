# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/GeoNode/geonode-importer/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                                        |    Stmts |     Miss |   Cover |   Missing |
|-------------------------------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| importer/\_\_init\_\_.py                                                                    |        8 |        0 |    100% |           |
| importer/api/\_\_init\_\_.py                                                                |        0 |        0 |    100% |           |
| importer/api/exception.py                                                                   |       37 |        0 |    100% |           |
| importer/api/serializer.py                                                                  |       16 |        0 |    100% |           |
| importer/api/urls.py                                                                        |        5 |        0 |    100% |           |
| importer/api/views.py                                                                       |      114 |        3 |     97% |188-189, 258 |
| importer/apps.py                                                                            |       13 |        0 |    100% |           |
| importer/celery\_app.py                                                                     |        4 |        0 |    100% |           |
| importer/celery\_tasks.py                                                                   |      226 |       23 |     90% |55, 150, 246-249, 379-389, 550-560, 599, 645-655, 715-725, 773-778 |
| importer/datastore.py                                                                       |       14 |        0 |    100% |           |
| importer/db\_router.py                                                                      |       18 |        0 |    100% |           |
| importer/handlers/\_\_init\_\_.py                                                           |        0 |        0 |    100% |           |
| importer/handlers/apps.py                                                                   |       25 |        3 |     88% |     79-83 |
| importer/handlers/base.py                                                                   |       96 |       19 |     80% |40, 53, 58, 64, 71, 96, 104, 112, 122, 130, 196, 203, 211, 225, 233, 242, 245, 253, 259 |
| importer/handlers/common/\_\_init\_\_.py                                                    |        0 |        0 |    100% |           |
| importer/handlers/common/metadata.py                                                        |       45 |        4 |     91% |35, 48, 66, 92 |
| importer/handlers/common/raster.py                                                          |      256 |       85 |     67% |41, 45, 65, 74, 90, 107-108, 133-139, 146-148, 151-163, 166-177, 194, 211, 228-240, 304-306, 335, 395-406, 458-464, 478-487, 497, 504-505, 511, 541, 547-553, 561-566, 569, 574, 592-627 |
| importer/handlers/common/vector.py                                                          |      379 |       53 |     86% |54, 73, 82, 142-143, 210-211, 268, 274-277, 385-389, 451-455, 485, 589, 656-667, 719-725, 786-787, 793, 828, 830-834, 843-848, 854-860, 868-873, 878, 883, 923-924, 985-986, 1001 |
| importer/handlers/csv/\_\_init\_\_.py                                                       |        0 |        0 |    100% |           |
| importer/handlers/csv/exceptions.py                                                         |        7 |        0 |    100% |           |
| importer/handlers/csv/handler.py                                                            |       94 |       22 |     77% |69, 99, 158-223, 229, 242, 255 |
| importer/handlers/geojson/\_\_init\_\_.py                                                   |        0 |        0 |    100% |           |
| importer/handlers/geojson/exceptions.py                                                     |        7 |        0 |    100% |           |
| importer/handlers/geojson/handler.py                                                        |       56 |        4 |     93% |59, 76-77, 93 |
| importer/handlers/geotiff/\_\_init\_\_.py                                                   |        0 |        0 |    100% |           |
| importer/handlers/geotiff/exceptions.py                                                     |        7 |        0 |    100% |           |
| importer/handlers/geotiff/handler.py                                                        |       32 |        1 |     97% |        57 |
| importer/handlers/gpkg/\_\_init\_\_.py                                                      |        0 |        0 |    100% |           |
| importer/handlers/gpkg/exceptions.py                                                        |        7 |        0 |    100% |           |
| importer/handlers/gpkg/handler.py                                                           |       53 |        5 |     91% |58, 68, 103, 114, 129 |
| importer/handlers/gpkg/tasks.py                                                             |        9 |        0 |    100% |           |
| importer/handlers/kml/\_\_init\_\_.py                                                       |        0 |        0 |    100% |           |
| importer/handlers/kml/exceptions.py                                                         |        7 |        0 |    100% |           |
| importer/handlers/kml/handler.py                                                            |       51 |        4 |     92% |58, 68, 105, 114 |
| importer/handlers/shapefile/\_\_init\_\_.py                                                 |        0 |        0 |    100% |           |
| importer/handlers/shapefile/exceptions.py                                                   |        7 |        0 |    100% |           |
| importer/handlers/shapefile/handler.py                                                      |       87 |        7 |     92% |63, 110, 138, 178, 188-190 |
| importer/handlers/shapefile/serializer.py                                                   |       19 |        0 |    100% |           |
| importer/handlers/sld/\_\_init\_\_.py                                                       |        0 |        0 |    100% |           |
| importer/handlers/sld/exceptions.py                                                         |        7 |        0 |    100% |           |
| importer/handlers/sld/handler.py                                                            |       26 |        3 |     88% | 25, 52-55 |
| importer/handlers/tiles3d/\_\_init\_\_.py                                                   |        0 |        0 |    100% |           |
| importer/handlers/tiles3d/exceptions.py                                                     |        7 |        0 |    100% |           |
| importer/handlers/tiles3d/handler.py                                                        |      132 |       23 |     83% |139, 148-199, 223, 251-254, 275, 297 |
| importer/handlers/tiles3d/utils.py                                                          |      107 |        2 |     98% |  139, 218 |
| importer/handlers/utils.py                                                                  |       58 |        8 |     86% |97-98, 111-112, 122-125 |
| importer/handlers/xml/\_\_init\_\_.py                                                       |        0 |        0 |    100% |           |
| importer/handlers/xml/exceptions.py                                                         |        7 |        0 |    100% |           |
| importer/handlers/xml/handler.py                                                            |       26 |        3 |     88% | 25, 51-54 |
| importer/handlers/xml/serializer.py                                                         |       12 |        0 |    100% |           |
| importer/migrations/0001\_initial.py                                                        |        6 |        0 |    100% |           |
| importer/migrations/0002\_resourcehandlerinfo\_kwargs.py                                    |        4 |        0 |    100% |           |
| importer/migrations/0003\_resourcehandlerinfo\_execution\_id.py                             |        5 |        0 |    100% |           |
| importer/migrations/0004\_rename\_execution\_id\_resourcehandlerinfo\_execution\_request.py |        4 |        0 |    100% |           |
| importer/migrations/0005\_fixup\_dynamic\_shema\_table\_names.py                            |       19 |        7 |     63% |     19-25 |
| importer/migrations/0006\_dataset\_migration.py                                             |       21 |       12 |     43% |     14-29 |
| importer/migrations/\_\_init\_\_.py                                                         |        0 |        0 |    100% |           |
| importer/models.py                                                                          |       23 |        2 |     91% |     28-29 |
| importer/orchestrator.py                                                                    |      147 |       16 |     89% |70-71, 102, 159-161, 191, 259-268, 272-273, 284, 292-295 |
| importer/publisher.py                                                                       |       74 |       14 |     81% |61, 91, 94-111, 146-148, 193-194 |
| importer/settings.py                                                                        |        6 |        0 |    100% |           |
| importer/tests/\_\_init\_\_.py                                                              |        0 |        0 |    100% |           |
| importer/tests/end2end/\_\_init\_\_.py                                                      |        0 |        0 |    100% |           |
| importer/tests/unit/\_\_init\_\_.py                                                         |        0 |        0 |    100% |           |
| importer/tests/utils.py                                                                     |       20 |        0 |    100% |           |
| importer/urls.py                                                                            |        0 |        0 |    100% |           |
| importer/utils.py                                                                           |       27 |        2 |     93% |    24, 27 |
| importer/views.py                                                                           |        0 |        0 |    100% |           |
| setup.py                                                                                    |        7 |        7 |      0% |      1-15 |
|                                                                                   **TOTAL** | **2444** |  **332** | **86%** |           |


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