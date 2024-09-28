#!/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

import oracledb
import psycopg2

###################
#### CONSTANTS ####
###################

#oracleDB CONFIG
ORACLE_HOSTNAME = "xxxxxxxxxxxxxxxx"
ORACLE_PORT = xxxxxxxxxxxxxxxx
ORACLE_DATABASE = "xxxxxxxxxxxxxxxx"
ORACLE_USERNAME = "xxxxxxxxxxxxxxxx"
ORACLE_PW = "xxxxxxxxxxxxxxxx"
ORACLE_CONNECT = f'{ORACLE_USERNAME}/{ORACLE_PW}@{ORACLE_HOSTNAME}:{ORACLE_PORT}/{ORACLE_DATABASE}'

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

def parse(entry):
    """returns a tuple containing a calls information."""
    mobile = False
    mobile_tag = "Mobile"
    if mobile_tag.lower() in entry[6].lower():
        mobile = True
        region = entry[6].split("M", 1)[0]
    else:
        region = entry[6].split(" ", 1)[0]

    if region.startswith("Swi"):
        region = "Switzerland"
    if entry[2].startswith("004175411"):
        region = "CERN_mobile"
    if region.startswith("France"):
        region = "France"
    result = (entry[7], entry[0], entry[4], region, entry[1], entry[2], entry[3], entry[5], mobile)
    return result

def create_table():
    """Creates the table in which the resulsting data will be stored."""
    create_query = """CREATE TABLE IF NOT EXISTS external_calls(
        ID SERIAL PRIMARY KEY NOT NULL,
        time TIMESTAMP,
        SIP_CALL_ID VARCHAR (80),
        USED_OPERATOR VARCHAR (32),
        REGION VARCHAR (32),
        SRC_USERNAME VARCHAR (32),
        DST_USERNAME VARCHAR (32),
        DURATION INT,
        COST float4,
        MOBILE BOOLEAN)
    """
    postgrecursor.execute(create_query)
    postgreconn.commit()

def get_last_date():
    """this function aims at retrieving the last recorded date in the target database"""
    postgrecursor.execute("SELECT time FROM external_calls ORDER BY time DESC LIMIT 1")
    results = postgrecursor.fetchone()
    if not results:
        date = "2010-01-01 00:00:00"
    else:
        date = results[0]
    return date

def send_data(data_list):
    """this function is inserting (sending) the data to the target database"""
    send_query = """
    INSERT INTO external_calls (time, SIP_CALL_ID, USED_OPERATOR, REGION, SRC_USERNAME, 
    DST_USERNAME, DURATION, COST, MOBILE)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    postgrecursor.executemany(send_query, data_list)
    postgreconn.commit()

##############
#### MAIN ####
##############

with oracledb.connect(dsn=ORACLE_CONNECT) as connection:
    with connection.cursor() as cursor:
        with postgreconn.cursor() as postgrecursor:
            create_table()
            lastDate = get_last_date()
            query = f"""
                SELECT SIP_CALL_ID, SRC_USERNAME, DST_USERNAME, DURATION, USED_OPERATOR, COST, REGION, CALL_START_TIME 
                FROM PHONE_BILLING.TONE_FE_CDRS 
                WHERE CALL_START_TIME > TO_DATE('{lastDate}', 'YYYY-MM-DD HH24:MI:SS')
                ORDER BY CALL_START_TIME ASC
                """
            data = []
            for row in cursor.execute(query):
                data.append(parse(row))
                if len(data) > 2000:
                    send_data(data)
                    data = []
            send_data(data)
