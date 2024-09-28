#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

import os
import ssl
import logging
from datetime import datetime

import psycopg2

import paho.mqtt.client as mqtt

from kafka import KafkaConsumer
from krbticket import KrbConfig, KrbCommand

from apscheduler.schedulers.background import BackgroundScheduler

####################
#### MONITORING ####
####################

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

###################
#### CONSTANTS ####
###################

THRESHOLD_SEC = int(os.getenv("THRESHOLD_SEC", 300))
INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", 60))

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_LISTEN_TOPIC = os.getenv("MQTT_LISTEN_TOPIC", "#")
MQTT_CA_CERT = os.getenv("MQTT_CA_CERT")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")
KAFKA_KRB_PRINCIPAL = os.getenv("KAFKA_KRB_PRINCIPAL")
KAFKA_KRB_KEYTAB = os.getenv("KAFKA_KRB_KEYTAB")

#postgreSQL CONFIG
postgreconn = psycopg2.connect(
    database=os.getenv("POSTGRESQL_DATABASE", "xxxxxxxx"),
    host=os.getenv("POSTGRESQL_HOSTNAME", "xxxxxxxx"),
    user=os.getenv("POSTGRESQL_USERNAME", "xxxxxxxx"),
    password=os.getenv("POSTGRESQL_PASSWORD"),
    port=os.getenv("POSTGRESQL_PORT", "xxxxxxxx")
)

###################
#### VARIABLES ####
###################

last_mqtt = datetime.now()
last_kafka = datetime.now()

mqtt_client = None
kafka_consumer = None

iot_interruption = False

#################
#### METHODS ####
#################


def on_connect_mqtt(client, userdata, flags, rc):
    """creates a message whenever the script connects to a MQTT topic"""
    logger.info("SUBSCRIBED TO %s WITH RC %s", MQTT_LISTEN_TOPIC, rc)
    mqtt_client.subscribe(MQTT_LISTEN_TOPIC)

def on_message_mqtt(client, userdata, message):
    """creates a message whenever the script receives a message by MQTT"""
    global last_mqtt
    global iot_interruption

    new_datetime = datetime.now()
    if iot_interruption:
        with postgreconn.cursor() as postgrecursor:
            update_data(postgrecursor, new_datetime)
        iot_interruption = False

    last_mqtt = new_datetime
    logger.info("Received MQTT message in %s", message.topic)

def on_message_kafka(message):
    """creates a message whenever the script receives a message by kafka"""
    global last_kafka
    last_kafka = datetime.now()
    logger.info("Received Kafka message in %s %s", message.topic, message.key)

def job():
    """calculates the time between two messages"""
    global iot_interruption
    now_sec = datetime.now()
    mqtt_secs = (now_sec - last_mqtt).total_seconds()
    kafka_secs = (now_sec - last_kafka).total_seconds()

    error_msg = None
    if mqtt_secs > THRESHOLD_SEC and kafka_secs > THRESHOLD_SEC:
        error_msg = f"No message received via neither MQTT nor Kafka for {mqtt_secs} / {kafka_secs} seconds"
    elif mqtt_secs > THRESHOLD_SEC:
        error_msg = f"No message received via MQTT for {mqtt_secs} seconds"
    elif kafka_secs > THRESHOLD_SEC:
        error_msg = f"No message received via Kafka for {kafka_secs} seconds"

    if mqtt_secs> 1800 and not iot_interruption:
        with postgreconn.cursor() as postgrecursor:
            insert_data(postgrecursor, last_mqtt)
            iot_interruption = True

    if error_msg:
        logger.error(error_msg)
    else:
        logger.debug("All ok")

def update_data(postgrecursor, end_time):
    """this function is inserting (sending) the end_time of an interruption to the target database"""
    sql = "UPDATE mqtt_interruption SET end_time = %s WHERE end_time IS NULL"
    postgrecursor.execute(sql, (end_time,))
    postgreconn.commit()

def insert_data(postgrecursor, start_time):
    """this function is inserting (sending) the data to the target database"""
    sql = "INSERT INTO mqtt_interruption(start_time, end_time) VALUES( %s, %s)"
    postgrecursor.execute(sql, (start_time, None))
    postgreconn.commit()

##############
#### MAIN ####
##############

def main():
    """main processing function"""
    # create the posgres table
    with postgreconn.cursor() as postgrecursor:
        postgrecursor.execute("""
            CREATE TABLE IF NOT EXISTS mqtt_interruption(
            ID SERIAL PRIMARY KEY NOT NULL,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            false_positive boolean DEFAULT FALSE);
            """)
        postgreconn.commit()

    # PERIODIC CHECK

    logger.info("SCHEDULING TASK EACH %d seconds to fail after %d seconds", INTERVAL_SEC, THRESHOLD_SEC)

    sched = BackgroundScheduler()
    sched.add_job(job, 'interval', id='check_messages', seconds=INTERVAL_SEC)
    sched.start()

    # MQTT CONNECTION

    logger.info("CONNECTING TO MQTT %s:%s", MQTT_HOST, MQTT_PORT)

    global mqtt_client
    mqtt_client = mqtt.Client(client_id="iot-e2e-testing")
    mqtt_client.on_connect = on_connect_mqtt
    mqtt_client.on_message = on_message_mqtt
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.tls_set(ca_certs=MQTT_CA_CERT, cert_reqs=ssl.CERT_NONE)
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    mqtt_client.loop_start()

    # KAFKA CONNECTION

    logger.info("CONNECTING TO KAFKA %s", KAFKA_TOPIC)

    kconfig = KrbConfig(principal=KAFKA_KRB_PRINCIPAL, keytab=KAFKA_KRB_KEYTAB)
    KrbCommand.kinit(kconfig)

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    global kafka_consumer
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS.split(','),
        group_id=KAFKA_GROUP,
        security_protocol='SASL_SSL',
        #ssl_cafile=MQTT_CA_CERT,
        ssl_context=ctx,
        sasl_mechanism='GSSAPI',
        sasl_kerberos_service_name='kafka',
        auto_offset_reset='earliest'
    )

    kafka_consumer.subscribe([KAFKA_TOPIC])

    for message in kafka_consumer:
        on_message_kafka(message)


if __name__ == "__main__":
    main()
