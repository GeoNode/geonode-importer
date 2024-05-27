#!/bin/bash
set -a
. ./.env_test
set +a 
coverage run --append --source='.' /usr/src/geonode/manage.py test importer.tests.end2end.test_end2end.ImporterNoCRSImportTest.test_import_geopackage_with_no_crs_table_should_raise_error_if_all_layer_are_invalid -v2 --noinput
