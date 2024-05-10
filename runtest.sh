#!/bin/bash
set -a
. ./.env_test
set +a 
coverage run --append --source='.' /usr/src/geonode/manage.py test importer.handlers.common.tests_vector.TestBaseVectorFileHandler.test_perform_last_step -v2 --keepdb
