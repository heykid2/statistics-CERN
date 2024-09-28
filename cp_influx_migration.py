#!/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

import psycopg2
from influxdb_client import InfluxDBClient

###################
#### CONSTANTS ####
###################

# INFLUXDB CONFIG
INFLUXDB_BUCKET_ID = "cp_log_stats"
INFLUXDB_ORG_ID = "cernphone_stats"
INFLUXDB_CONFIG_FILE = "/home/itcstrtesting/Downloads/influxdb.config"
INFLUX_CLIENT = InfluxDBClient.from_config_file(INFLUXDB_CONFIG_FILE)

#postgreSQL CONFIG
postgreconn = psycopg2.connect(
    database=os.getenv("POSTGRESQL_DATABASE", "xxxxxxxx"),
    host=os.getenv("POSTGRESQL_HOSTNAME", "xxxxxxxx"),
    user=os.getenv("POSTGRESQL_USERNAME", "xxxxxxxx"),
    password=os.getenv("POSTGRESQL_PASSWORD"),
    port=os.getenv("POSTGRESQL_PORT", "xxxxxxxx")
)

###########################
#### AUXILIARY METHODS ####
###########################

def send_register(postgrecursor, data):
    # this function is inserting (sending) the data to the target database
    sql = 'INSERT INTO register(time, extension, client_platform, client_version, user_agent) VALUES( %s, %s, %s, %s, %s)'
    postgrecursor.executemany(sql, data)
    postgreconn.commit()

def send_invite(postgrecursor, data):
    # this function is inserting (sending) the data to the target database
    sql = 'INSERT INTO invite(time, source, destination, call_id, client_platform, client_version, source_type, destination_type, user_agent) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    postgrecursor.executemany(sql, data)
    postgreconn.commit()

##############
#### MAIN ####
##############

querier = INFLUX_CLIENT.query_api()
with postgreconn.cursor() as postgrecursor:

    ##################
    #### REGISTER ####
    ##################

    # create the posgres "register" table
    postgrecursor.execute("""CREATE TABLE IF NOT EXISTS register(
        ID SERIAL PRIMARY KEY NOT NULL,
        time TIMESTAMP,
        extension INT,
        client_platform VARCHAR(256),
        client_version VARCHAR(256),
        user_agent VARCHAR(256));
        """)
    postgreconn.commit()
    # fetching the data form influx
    query = f"""from(bucket:"{INFLUXDB_BUCKET_ID}") 
            |> range(start: -5y, stop: now())
            |> filter(fn: (r) => r._measurement == "register")
            |> filter(fn: (r) => r._field == "user_agent")
             """
    tables = querier.query(query)
    data = []      
    i = 0
    if tables:
        data = tables.to_values(columns=["_time", "extension", "client_platform", "client_version"])
        for table in tables:
            for record in table.records:
                data[i].append((record.get_value()))  
                i += 1
    # pushing the fetched data to postgres
    send_register(postgrecursor, data)
    del data

    ################
    #### INVITE ####
    ################

    # create the posgres "invite" table
    postgrecursor.execute("""CREATE TABLE IF NOT EXISTS invite(
        ID SERIAL PRIMARY KEY NOT NULL,
        time TIMESTAMP,
        source VARCHAR(256),
        destination VARCHAR(256),
        call_id VARCHAR(256),
        client_platform VARCHAR(256),
        client_version VARCHAR(256),
        source_type VARCHAR(256),
        destination_type VARCHAR(256),
        user_agent VARCHAR(256));
        """)
    postgreconn.commit()
    # fetching the data form influx
    query = f"""from(bucket:"{INFLUXDB_BUCKET_ID}") 
            |> range(start: -5y, stop: now())
            |> filter(fn: (r) => r._measurement == "invite")
            |> filter(fn: (r) => r._field == "source_type")
             """
    tables = querier.query(query)
    data = []      
    i = 0
    if tables:
        data = tables.to_values(columns=["_time", "source", "destination", "call_id", "client_platform", "client_version"])

    for table in tables:
        for record in table.records:
            tmp = record.get_value()
            data[i].append(tmp)
            i += 1
    i = 0
    query_2 = f"""from(bucket:"{INFLUXDB_BUCKET_ID}") 
            |> range(start: -5y, stop: now())
            |> filter(fn: (r) => r._measurement == "invite")
            |> filter(fn: (r) => r._field == "destination_type")
             """
    tables = querier.query(query_2)
    for table in tables:
        for record in table.records:
            tmp = record.get_value()
            data[i].append(tmp)
            i += 1
        
    i = 0
    query_3 = f"""from(bucket:"{INFLUXDB_BUCKET_ID}") 
            |> range(start: -5y, stop: now())
            |> filter(fn: (r) => r._measurement == "invite")
            |> filter(fn: (r) => r._field == "user_agent")
             """
    tables = querier.query(query_3)
    for table in tables:
        for record in table.records:
            tmp = record.get_value()
            data[i].append(tmp)
            i += 1
    
    # pushing the fetched data to postgres
    send_invite(postgrecursor, data)
    del data