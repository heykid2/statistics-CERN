#!/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

import csv
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

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

FILENAME = "unregistered_callers.csv"
SENDER_ADDRESS = "cernphone-resources@cern.ch"
RECIPIENT_ADDRESS = "loeiz.badouel@cern.ch"
SMTPSERVER = "cernmx.cern.ch"
SMTPPORT = 25

###########################
#### AUXILIARY METHODS ####
###########################

def send_email(subject, body, from_email_addr = SENDER_ADDRESS, destination_addr = RECIPIENT_ADDRESS, cc = None, files = None):
    """Creates and send the email containing the script's results."""
    if cc is None:
        cc = []
    if files is None:
        files = []
    try:
        mssg = MIMEMultipart()
        mssg['Subject'] = subject
        mssg['From'] = from_email_addr
        mssg['To'] = destination_addr
        mssg['Cc'] = ', '.join(cc)

        mssg.attach(MIMEText(body, "html"))

        for path in files:
            part = MIMEBase('application', "octet-stream")
            with open(path, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; FILENAME={}'.format(Path(path).name))
            mssg.attach(part)

        to_addresses = [mssg['To']] + cc
        with SMTP(SMTPSERVER, SMTPPORT) as server:
            server.starttls()
            server.sendmail(from_email_addr, to_addresses , mssg.as_string())
    except Exception as e:
        raise e

##############
#### MAIN ####
##############

final_list = []

with postgreconn.cursor() as postgrecursor:
    SELECT_POSTGRES = """
        SELECT DISTINCT extension
        FROM register
        WHERE time > now() - interval '1 year'
        """
    postgrecursor.execute(SELECT_POSTGRES)
    list_from_postgres = {str(row[0]) for row in postgrecursor.fetchall()}

    with oracledb.connect(dsn=ORACLE_CONNECT) as connection:
        with connection.cursor() as cursor:
            SELECT_ORACLE = """
                SELECT TELCFPIN, STDCFPIN 
                FROM CS_FOUNDATION_PUB.CFPIN_HISTO 
                WHERE endcfpin is NULL and TLTCFPIN like 'T' and TSTCFPIN like '%DT' AND STDCFPIN < sysdate AND (ENDCFPIN is null or ENDCFPIN > sysdate)
                """

            for row in cursor.execute(SELECT_ORACLE):
                if row[0] not in list_from_postgres:
                    result = "Never"

                    query = f"""
                        SELECT time
                        FROM register
                        WHERE extension = '{row[0]}'
                        ORDER BY time DESC
                        LIMIT 1
                        """
                    postgrecursor.execute(query)
                    results = postgrecursor.fetchall()
                    if results:
                        result = results[-1][0]

                    row_list = (row[0], row[1], result)
                    final_list.append(row_list)

with open(FILENAME, 'w', encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([ "extension", "Profile Creation Date", "Last Registration Date"])
    writer.writerows(final_list)

SUBJECT = "Updated list of the unregistered users with the date of the corresponding call"
BODY = """You will find as an enclosure a csv file containing the list of the unregistered callers
    with the date of the corresponding call."""
send_email( SUBJECT, BODY, SENDER_ADDRESS, RECIPIENT_ADDRESS, files=[FILENAME])
