#!/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

from datetime import datetime

import psycopg2

###################
#### CONSTANTS ####
###################

#postgreSQL CONFIG
postgreconn_fetch = psycopg2.connect(
    database=os.getenv("POSTGRESQL_DATABASE", "xxxxxxxx"),
    host=os.getenv("POSTGRESQL_HOSTNAME", "xxxxxxxx"),
    user=os.getenv("POSTGRESQL_USERNAME", "xxxxxxxx"),
    password=os.getenv("POSTGRESQL_PASSWORD"),
    port=os.getenv("POSTGRESQL_PORT", "xxxxxxxx")
)

#postgreSQL CONFIG
postgreconn_store = psycopg2.connect(
    database=os.getenv("POSTGRESQL_DATABASE", "xxxxxxxx"),
    host=os.getenv("POSTGRESQL_HOSTNAME", "xxxxxxxx"),
    user=os.getenv("POSTGRESQL_USERNAME", "xxxxxxxx"),
    password=os.getenv("POSTGRESQL_PASSWORD"),
    port=os.getenv("POSTGRESQL_PORT", "xxxxxxxx")
)

SELECT_NUMBER_DEVICES = "SELECT count(*) FROM device_restricted_view_grafana"
SELECT_NUMBER_APPLICATION = "SELECT count(*) FROM application_restricted_view_grafana"
SELECT_NUMBER_GATEWAY = "SELECT count(*) FROM gateway_restricted_view_grafana"

CREATE_TABLE_NUMBER_DEVICES = """CREATE TABLE IF NOT EXISTS number_devices(
    date TIMESTAMP PRIMARY KEY NOT NULL,
    number INT)
"""

CREATE_TABLE_NUMBER_APPLICATION = """CREATE TABLE IF NOT EXISTS number_applications(
    date TIMESTAMP PRIMARY KEY NOT NULL,
    number INT)
"""

CREATE_TABLE_NUMBER_GATEWAY = """CREATE TABLE IF NOT EXISTS number_gateway(
    date TIMESTAMP PRIMARY KEY NOT NULL,
    number INT)
"""

SEND_DATA_NUMBER_DEVICES = "INSERT INTO number_devices( date, number) VALUES(%s, %s)"
SEND_DATA_NUMBER_APPLICATION = "INSERT INTO number_applications( date, number) VALUES(%s, %s)"
SEND_DATA_NUMBER_GATEWAY = "INSERT INTO number_gateway( date, number) VALUES(%s, %s)"

EXECUTION_DATE = datetime.now()

###################
#### VARIABLES ####
###################

results_number_devices= ()
results_number_application= ()
results_number_gateway= ()

##############
#### MAIN ####
##############

with postgreconn_fetch.cursor() as postgrecursor_fetch:
    # first part
    postgrecursor_fetch.execute(SELECT_NUMBER_DEVICES)
    results_number_devices = (EXECUTION_DATE, postgrecursor_fetch.fetchone()[0])

    # second part
    postgrecursor_fetch.execute(SELECT_NUMBER_APPLICATION)
    results_number_application = (EXECUTION_DATE, postgrecursor_fetch.fetchone()[0])

    # third part
    postgrecursor_fetch.execute(SELECT_NUMBER_GATEWAY)
    results_number_gateway = (EXECUTION_DATE, postgrecursor_fetch.fetchone()[0])

postgreconn_fetch.close()

with postgreconn_store.cursor() as postgrecursor_store:
    # creating the tables
    postgrecursor_store.execute(CREATE_TABLE_NUMBER_DEVICES)
    postgreconn_store.commit()
    postgrecursor_store.execute(SEND_DATA_NUMBER_DEVICES, results_number_devices)
    postgreconn_store.commit()

    postgrecursor_store.execute(CREATE_TABLE_NUMBER_APPLICATION)
    postgreconn_store.commit()
    postgrecursor_store.execute(SEND_DATA_NUMBER_APPLICATION, results_number_application)
    postgreconn_store.commit()

    postgrecursor_store.execute(CREATE_TABLE_NUMBER_GATEWAY)
    postgreconn_store.commit()
    postgrecursor_store.execute(SEND_DATA_NUMBER_GATEWAY, results_number_gateway)
    postgreconn_store.commit()

postgreconn_store.close()
