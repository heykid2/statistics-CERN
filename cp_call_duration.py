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

def get_last_date():
    """this function aims at retrieving the last recorded date in the target database"""
    postgrecursor.execute("SELECT CALLDATE FROM stats_cernphone ORDER BY CALLDATE DESC LIMIT 1")
    results = postgrecursor.fetchone()
    if not results:
        date = "2022-06-01 00:00:00"
    else:
        date = results[0]
    return date

def send_data(data_list):
    """this function is inserting (sending) the data to the target database"""
    send_query = 'INSERT INTO stats_cernphone(CALLDATE, BILLSEC, SRC, DST, DCONTEXT, DISPOSITION, DURATION) VALUES(%s, %s, %s, %s, %s, %s, %s)'
    postgrecursor.executemany(send_query, data_list)
    postgreconn.commit()

##############
#### MAIN ####
##############

with oracledb.connect(dsn=ORACLE_CONNECT) as connection:
    #this script creates (if it doesn't exist) the target database then is fetching from the
    #original database the data which is stored temporarily (200 rows) and then sent as a batch
    with connection.cursor() as cursor:
        with postgreconn.cursor() as postgrecursor:
            postgrecursor.execute("""CREATE TABLE IF NOT EXISTS stats_cernphone(
                ID SERIAL PRIMARY KEY NOT NULL,
                CALLDATE TIMESTAMP,
                BILLSEC INT,
                SRC VARCHAR (255),
                DST VARCHAR (255),
                DCONTEXT VARCHAR (255),
                DISPOSITION VARCHAR (255),
                DURATION INT);
                """)
            lastTime = get_last_date()
            query = f"""
                SELECT CALLDATE, BILLSEC, SRC, DST, DCONTEXT, DISPOSITION, DURATION FROM PHONE_BILLING.TONE_RE_TICKETS 
                WHERE NODE LIKE 'cernphone%' 
                AND CALLDATE > TO_DATE('{lastTime}','YYYY-MM-DD HH24:MI:SS')
                """
            data = []
            for row in cursor.execute(query):
                if row[2] and len(row[2]) > 5:
                    src = list(row[2])
                    src[-1] = "X"
                    src[-2] = "X"
                    src[-3] = "X"
                    src[-4] = "X"
                    src_final = "".join(str(x) for x in src)
                else:
                    src_final = row[2]
                if row[3] and len(row[3]) > 5:
                    dst = list(row[3])
                    dst[-1] = "X"
                    dst[-2] = "X"
                    dst[-3] = "X"
                    dst[-4] = "X"
                    dst_final = "".join(str(x) for x in dst)
                else:
                    dst_final = row[3]

                data.append((row[0], row[1], src_final, dst_final, row[4], row[5], row[6]))
                if len(data) > 200:
                    send_data(data)
                    data = []
            send_data(data)
