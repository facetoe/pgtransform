#!/usr/bin/env bash

# Simple script for building tutorial environment.
# WARNING: drops databases before creating them!
# Requires postgres user is able to connect without a password.


EXAMPLEDB_ZIP_PATH=dvdrental.zip
EXAMPLEDB_TAR_PATH=dvdrental.tar

TEMPLATEDB_NAME=dvdrental
TESTDB_NAME=dvdrental_modified

if [ ! -f ${EXAMPLEDB_ZIP_PATH} ]; then
    wget http://www.postgresqltutorial.com/download/dvd-rental-sample-database/?wpdmdl=969 -O dvdrental.zip
fi

if [ ! -f  ${EXAMPLEDB_TAR_PATH} ]; then
    echo "unzipping: $EXAMPLEDB_ZIP_PATH"
    unzip ${EXAMPLEDB_ZIP_PATH}
fi

# Create our template database
echo "creating template database: $TEMPLATEDB_NAME"
dropdb ${TEMPLATEDB_NAME} 2> /dev/null || true
createdb ${TEMPLATEDB_NAME} -O postgres
pg_restore -d ${TEMPLATEDB_NAME} -U postgres ${EXAMPLEDB_TAR_PATH}

# Create our test databases
echo "creating test database: $TESTDB_NAME"
dropdb ${TESTDB_NAME} 2> /dev/null || true
createdb -O postgres -T ${TEMPLATEDB_NAME} ${TESTDB_NAME}
