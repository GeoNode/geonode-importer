#!/bin/bash
set -a
. ./.env_test
set +a 

coverage run --source='.' --omit="*/test*" /usr/src/geonode/manage.py test importer.tests.end2end.test_end2end -v2 --noinput
