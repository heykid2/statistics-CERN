#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

import ssl
import logging

from kafka import KafkaConsumer
from krbticket import KrbConfig, KrbCommand

####################
#### MONITORING ####
####################

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

###################
#### CONSTANTS ####
###################

KAFKA_BROKERS = "nile-kafka-gp3-a01:9093,nile-kafka-gp3-a02:9093,nile-kafka-gp3-b01:9093,nile-kafka-gp3-b02:9093,nile-kafka-gp3-c01:9093,nile-kafka-gp3-c02:9093"
KAFKA_TOPIC = "lora-liveness-test"
KAFKA_GROUP = "iot-service-interruption"
KAFKA_KRB_PRINCIPAL = "iote2etesting@CERN.CH"
KAFKA_KRB_KEYTAB = "/keytab"

##############
#### MAIN ####
##############

logger.info("CONNECTING TO KAFKA %s", KAFKA_TOPIC)

kconfig = KrbConfig(principal=KAFKA_KRB_PRINCIPAL, keytab=KAFKA_KRB_KEYTAB)
KrbCommand.kinit(kconfig)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

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
    logger.info("Received Kafka message in %s %s", message.topic, message.key)
