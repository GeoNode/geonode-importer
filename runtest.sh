#!/bin/bash
set -a
. ./.env_test
set +a 

coverage run --source='.' --omit="*/test*" /usr/src/geonode/manage.py test importer.tests -v2 --noinput --keepdb
