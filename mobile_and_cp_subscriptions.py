#!/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

from datetime import datetime
from dateutil.relativedelta import relativedelta

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

def send_data(data, service):
    """this function is inserting (sending) the data to the target database."""
    query = f"INSERT INTO {service}_subscriptions( MONTH, AMOUNT_SUBSCRIPTION) VALUES( %s, %s)"
    postgrecursor.executemany(query, data)
    postgreconn.commit()

def data_process(row):
    """takes a subscription as entry and adds 1 in every month that this description is active."""
    month_str = row[1].strftime("%Y-%m")
    if row[2]:
        month_end_str = row[2].strftime("%Y-%m")
    else:
        month_end_str = datetime.now().strftime("%Y-%m")

    if month_str not in variable_dict:
        variable_dict[month_str] = []
    variable_dict[month_str].append(row[0])
    if month_end_str != month_str:
        month_start = datetime.strptime(month_str, "%Y-%m")
        month_end = datetime.strptime(month_end_str, "%Y-%m")
        while month_start < month_end:
            month_start = month_start + relativedelta(months=1)
            month_str = month_start.strftime("%Y-%m")
            if month_str not in variable_dict:
                variable_dict[month_str] = []
            if row[0] not in variable_dict[month_str]:
                variable_dict[month_str].append(row[0])

def send_data_secondary(data, table, column):
    """this function is inserting (sending) the data to the right database."""
    query = f"INSERT INTO {table}( {column}, AMOUNT) VALUES( %s, %s)"
    postgrecursor.executemany(query, data)
    postgreconn.commit()

##############
#### MAIN ####
##############

with postgreconn.cursor() as postgrecursor:
    #Dropping the previously filled tables
    postgrecursor.execute("DROP TABLE mobile_subscriptions;")
    postgrecursor.execute("DROP TABLE cernphone_subscriptions;")
    postgrecursor.execute("DROP TABLE current_subscriptions;")
    postgrecursor.execute("DROP TABLE mobile_subscription_category;")
    postgrecursor.execute("DROP TABLE cernphone_subscription_category;")
    postgreconn.commit()
    # creating the new tables that are to be populated
    postgrecursor.execute("""
        CREATE TABLE IF NOT EXISTS mobile_subscriptions(
        MONTH TIMESTAMP PRIMARY KEY NOT NULL,
        AMOUNT_SUBSCRIPTION INT);
        """)
    postgrecursor.execute("""
        CREATE TABLE IF NOT EXISTS cernphone_subscriptions(
        MONTH TIMESTAMP PRIMARY KEY NOT NULL,
        AMOUNT_SUBSCRIPTION INT);
        """)
    postgrecursor.execute("""
        CREATE TABLE IF NOT EXISTS current_subscriptions(
        TSTCFPIN VARCHAR(20) PRIMARY KEY NOT NULL,
        AMOUNT INT);
        """)
    postgrecursor.execute("""
        CREATE TABLE IF NOT EXISTS mobile_subscription_category(
        CATCFPIN VARCHAR(20) PRIMARY KEY NOT NULL,
        AMOUNT INT);
        """)
    postgrecursor.execute("""
        CREATE TABLE IF NOT EXISTS cernphone_subscription_category(
        CATCFPIN VARCHAR(20) PRIMARY KEY NOT NULL,
        AMOUNT INT);
        """)
    postgreconn.commit()
    with oracledb.connect(dsn=ORACLE_CONNECT) as connection:
        with connection.cursor() as cursor:
            # First part
            # retrieving the amount of active subscriptions in every months (mobile)
            variable_dict = {}
            query = """
                SELECT TELCFPIN, STDCFPIN, ENDCFPIN 
                FROM CS_FOUNDATION_PUB.CFPIN_HISTO 
                WHERE TLTCFPIN = 'PP'
                """
            data = []
            for row in cursor.execute(query):
                data_process(row)
            for month, value in variable_dict.items():
                month_datetime = datetime.strptime(month, "%Y-%m")
                data.append((month_datetime, len(value)))
            send_data(data, "mobile")
            # Second part
            # retrieving the amount of active subscriptions in every months (cernphone)
            variable_dict = {}
            query = """
                SELECT TELCFPIN, STDCFPIN, ENDCFPIN 
                FROM CS_FOUNDATION_PUB.CFPIN_HISTO 
                WHERE TLTCFPIN = 'T' AND TSTCFPIN LIKE '%_DT'
                """
            data = []
            for row in cursor.execute(query):
                data_process(row)
            for month, value in variable_dict.items():
                month_datetime = datetime.strptime(month, "%Y-%m")
                data.append((month_datetime, len(value)))
            send_data(data, "cernphone")
            # third part
            # Retrieving the distribution between personal and shared numbers (cernphone)
            query = """
                SELECT TSTCFPIN, COUNT(*) 
                FROM CS_FOUNDATION_PUB.CFPIN_HISTO 
                WHERE TLTCFPIN = 'T' AND ENDCFPIN IS NULL AND TSTCFPIN LIKE '%DT' 
                GROUP BY TSTCFPIN
                """
            data = []
            for row in cursor.execute(query):
                tmp = (row[0], row[1])
                data.append(tmp)
            send_data_secondary(data, "current_subscriptions", "TSTCFPIN")
            # fourth part
            # Retrieving the distribution of categories (mobile)
            query = """
                SELECT CATCFPIN, COUNT(*) 
                FROM CS_FOUNDATION_PUB.CFPIN_HISTO 
                WHERE TLTCFPIN = 'PP' AND ENDCFPIN IS NULL 
                GROUP BY CATCFPIN
                """
            data = []
            for row in cursor.execute(query):
                tmp = (row[0], row[1])
                data.append(tmp)
            send_data_secondary(data, "mobile_subscription_category", "CATCFPIN")
            # fifth part
            # Retrieving the distribution of categories (cernphone)
            query = """
                SELECT CATCFPIN, COUNT(*) 
                FROM CS_FOUNDATION_PUB.CFPIN_HISTO 
                WHERE TLTCFPIN = 'T' AND ENDCFPIN IS NULL 
                GROUP BY CATCFPIN
                """
            data = []
            for row in cursor.execute(query):
                tmp = (row[0], row[1])
                data.append(tmp)
            send_data_secondary(data, "cernphone_subscription_category", "CATCFPIN")
