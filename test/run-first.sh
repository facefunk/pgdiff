#!/usr/bin/env bash

# Run this script before testing db_test.go.
#
# We need a Postgres user account to allow us to create the databases to test against. Running this script should set
# this user up for you; after that further logging in should not be required.
#
# In the interest of security, we don't actually create a fully-fledged superuser. Instead we create a role with all the
# necessary permissions to act as a superuser for the purposes of testing pgdiff against live databases but without risk
# to any third-party data hosted on the same server.

sql="CREATE ROLE pgdiff_parent PASSWORD 'asdf' NOSUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;
CREATE DATABASE pgdiff_parent OWNER = pgdiff_parent TEMPLATE = template0;"
sudo -u postgres psql <<< "$sql"
