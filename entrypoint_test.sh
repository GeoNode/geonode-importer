#!/bin/bash
export PGPASSWORD=postgres
cmd="$@"
RESULT=$(psql -d geonode --host db --username postgres -c "SELECT count(*) FROM pg_database WHERE datname = 'test_geonode'")

echo $RESULT

#psql -d geonode --host db --username postgres -c 'CREATE USER geonode SUPERUSER;'
#psql -d geonode --host db --username postgres -c 'ALTER USER geonode CREATEDB;'
#psql -d geonode_data --host db --username postgres -c 'CREATE USER geonode SUPERUSER;'
#psql -d geonode_data --host db --username postgres -c 'ALTER USER geonode CREATEDB;'

echo "creating"
psql -d geonode --host db --username postgres -c 'DROP DATABASE test_geonode'
psql -d geonode --host db --username postgres -c 'CREATE DATABASE test_geonode'
psql -d test_geonode --host db --username postgres -c 'ALTER USER geonode SUPERUSER;'
psql -d test_geonode --host db --username postgres -c 'ALTER USER geonode CREATEDB;'
psql -d test_geonode --host db --username postgres -c 'CREATE EXTENSION postgis;'
psql -d test_geonode --host db --username postgres -c 'GRANT ALL ON geometry_columns TO PUBLIC;'
psql -d test_geonode --host db --username postgres -c 'GRANT ALL ON spatial_ref_sys TO PUBLIC;'
psql -d test_geonode --host db --username postgres -c 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO geonode;'
echo "one done"
psql -d geonode_data --host db --username postgres -c 'DROP DATABASE test_geonode_data'
psql -d geonode_data --host db --username postgres -c 'CREATE DATABASE test_geonode_data'
psql -d test_geonode_data --host db --username postgres -c 'ALTER USER geonode_data SUPERUSER;'
psql -d test_geonode_data --host db --username postgres -c 'ALTER USER geonode_data CREATEDB;'
psql -d test_geonode_data --host db --username postgres -c 'CREATE EXTENSION postgis;'
psql -d test_geonode_data --host db --username postgres -c 'GRANT ALL ON geometry_columns TO PUBLIC;'
psql -d test_geonode_data --host db --username postgres -c 'GRANT ALL ON spatial_ref_sys TO PUBLIC;'
psql -d test_geonode_data --host db --username postgres -c 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO geonode_data;'
echo "Done"
