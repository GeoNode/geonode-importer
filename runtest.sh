#!/bin/bash
set -a
. ./.env_test
set +a 
coverage run --append --source='.' /usr/src/geonode/manage.py test importer --keepdb -v2 --noinput