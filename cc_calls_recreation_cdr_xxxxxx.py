#!/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

import psycopg2
import oracledb

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

list_cc = [('2023-12-06 00:00:00', '74444', '2024-02-20 00:00:00'),\
            ('2023-12-06 00:00:00', '74848', '2024-02-20 00:00:00'),\
            ('2023-11-27 00:00:00', '77777', '2024-02-20 00:00:00'),\
            ('2023-09-18 00:00:00', '75555', '2024-02-20 00:00:00'),\
            ('2023-09-18 00:00:00', '76666', '2024-02-20 00:00:00'),\
            ('2023-09-18 00:00:00', '66666', '2024-02-20 00:00:00'),\
            ('2023-12-04 00:00:00', '72201', '2024-02-20 00:00:00')]

###########################
#### AUXILIARY METHODS ####
###########################

def process_data(row_cdr, data_list):
    """this function process the data from CDR_lOG then from QUEUE_LOG"""
    queue_log_query = f"""
        select TIME, QUEUENAME, AGENT, EVENT, DATA, 
        CASE 
            WHEN EVENT = 'BLINDTRANSFER' THEN 1 
            WHEN EVENT = 'ATTENDEDTRANSFER' THEN 1 
            WHEN EVENT = 'ENTERQUEUE' THEN 2 
            WHEN EVENT = 'CONNECT' THEN 5 
            WHEN EVENT = 'RINGNOANSWER' THEN 3 
            WHEN EVENT = 'RINGCANCELLED' THEN 4 
            ELSE 6 END AS TEST 
        FROM CALLCENTER_PROD.QUEUE_LOG_{cc[1]}_NODE_MAIN 
        WHERE CALLID = '{row_cdr[0]}'
        UNION 
        select TIME, QUEUENAME, AGENT, EVENT, DATA, 
        CASE 
            WHEN EVENT = 'BLINDTRANSFER' THEN 1 
            WHEN EVENT = 'ATTENDEDTRANSFER' THEN 1 
            WHEN EVENT = 'ENTERQUEUE' THEN 2 
            WHEN EVENT = 'CONNECT' THEN 5 
            WHEN EVENT = 'RINGNOANSWER' THEN 3 
            WHEN EVENT = 'RINGCANCELLED' THEN 4 
            ELSE 6 END AS TEST 
        FROM CALLCENTER_PROD.QUEUE_LOG_{cc[1]}_NODE_BACKUP 
        WHERE CALLID = '{row_cdr[0]}' 
        ORDER BY TEST
        """
    dcontext = row_cdr[8]
    lastapp = row_cdr[9]
    disposition = row_cdr[6]
    duration_ringing = 0
    duration_queue = 0
    duration_talk = 0
    total_length = 0
    transfer = 0
    data_1 = 0
    data_2 = 0
    data_3 = 0
    destination = None

    with connection.cursor() as oraclecursor2:
        oraclecursor2.execute(queue_log_query)
        rows = oraclecursor2.fetchall()
        if len(rows) == 0:
            duration_ringing = row_cdr[4] - row_cdr[5]
            duration_talk = row_cdr[5]
            if row_cdr[7] is not None:
                transfer += 2
        else:
            for row_queue_log in rows:
                if row_queue_log[4]:
                    try:
                        if not row_queue_log[3].startswith("RINGNOAN"):
                            data_split = row_queue_log[4].split("|")
                            data_1 = data_split[0]
                            data_2 = data_split[1]
                            data_3 = data_split[2]
                    except IndexError:
                        pass

                    if row_queue_log[3].startswith("AB"):
                        duration_ringing = int(data_3)
                        if disposition == "FAILED":
                            disposition = "NO ANSWER"
                    elif row_queue_log[3].startswith("RINGNOAN"):
                        duration_ringing = int(row_queue_log[4])
                    elif row_queue_log[3].startswith("CON"):
                        duration_ringing = int(data_3)
                        duration_queue = int(data_1) - duration_ringing
                    elif row_queue_log[3].startswith("COM"):
                        duration_talk = int(data_2)
                    elif row_queue_log[3].endswith("TRANSFER") and transfer == 0:
                        transfer += 1

                if data and data[-1][8] == row_cdr[0]:
                    if not destination:
                        destination = data[-1][2]
                    data.pop()

        total_length = duration_ringing + duration_talk + duration_queue
        if not destination:
            destination = row_cdr[3]
        tmp_tuple = (row_cdr[1], row_cdr[2], destination, duration_queue, duration_ringing, duration_talk, total_length, disposition, row_cdr[0], transfer, dcontext, lastapp)
        data_list.append(tmp_tuple)

def send_data(data_list):
    """this function is inserting (sending) the data to the target database"""
    send_query = """
    INSERT INTO recreated_calls( CALLDATE, SRC, DST, DURATION_QUEUE, DURATION_RINGING, DURATION_TALK, CALL_LENGTH, DISPOSITION, UNIQUEID, TRANSFER, DCONTEXT, LASTAPP) 
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    postgrecursor.executemany(send_query, data_list)
    postgreconn.commit()

##############
#### MAIN ####
##############

with oracledb.connect(dsn=ORACLE_CONNECT) as connection:
    with connection.cursor() as oraclecursor:
        with postgreconn.cursor() as postgrecursor:
            for cc in list_cc:
                variable_dict = {}
                query = f'''
                select UNIQUEID, CALLDATE, SRC, DST, DURATION, BILLSEC, DISPOSITION, PEERACCOUNT, DCONTEXT, LASTAPP, SEQUENCE 
                FROM CALLCENTER_PROD.CDR_{cc[1]}_NODE_MAIN 
                WHERE 
                    CALLDATE > TO_DATE('{cc[0]}', 'YYYY-MM-DD HH24:MI:SS') AND CALLDATE < TO_DATE('{cc[2]}', 'YYYY-MM-DD HH24:MI:SS')
                    AND CHANNEL LIKE 'P%'
                UNION 
                select UNIQUEID, CALLDATE, SRC, DST, DURATION, BILLSEC, DISPOSITION, PEERACCOUNT, DCONTEXT, LASTAPP, SEQUENCE 
                FROM CALLCENTER_PROD.CDR_{cc[1]}_NODE_BACKUP 
                WHERE 
                    CALLDATE > TO_DATE('{cc[0]}', 'YYYY-MM-DD HH24:MI:SS') AND CALLDATE < TO_DATE('{cc[2]}', 'YYYY-MM-DD HH24:MI:SS')
                    AND CHANNEL LIKE 'P%' 
                ORDER BY 
                    UNIQUEID ASC, SEQUENCE ASC
                '''
                data = []
                for row_cdr in oraclecursor.execute(query):
                    if not (row_cdr[6].startswith("FAIL") and row_cdr[4]==0 and row_cdr[5]==0):
                        process_data(row_cdr, data)
                    if len(data) > 200:
                        send_data(data)
                        data = []
                send_data(data)
