#!/bin/env python3
# -*- coding: utf-8 -*-

#################
#### IMPORTS ####
#################

import glob
import gzip
import re
import mmap
import logging
import logging.handlers
import os
import sys
from enum import Enum
from datetime import datetime
from functools import lru_cache

import dateutil.parser

import psycopg2

###################
#### CONSTANTS ####
###################

# BASE PATH

BASE_PATH = "/eos/project/t/tone/cp_stats"

#postgreSQL CONFIG
postgreconn = psycopg2.connect(
    database=os.getenv("POSTGRESQL_DATABASE", "xxxxxxxx"),
    host=os.getenv("POSTGRESQL_HOSTNAME", "xxxxxxxx"),
    user=os.getenv("POSTGRESQL_USERNAME", "xxxxxxxx"),
    password=os.getenv("POSTGRESQL_PASSWORD"),
    port=os.getenv("POSTGRESQL_PORT", "xxxxxxxx")
)

INVITE_TABLE = "invite"
REGISTER_TABLE = "register"

# INPUT FILES

GZIP_EXTENSION = ".gz"

MAIN_LOG_FOLDER = f"{BASE_PATH}/workdir"
CURRENT_LOG_FILE = f"{MAIN_LOG_FOLDER}/*kamailio"

# TAGS AND PATTERNS

REGISTER_TAG = "---REGISTER"
INVITE_TAG = "---INVITE"

REGISTER_PATTERN = f"(.*) {REGISTER_TAG} (?P<extension>\d{{5}}) -> (\d{{5}})(.*)UA: (?P<user_agent>.*?),(.*)"
INVITE_PATTERN = f"(.*)\[1>INVITE (\d*) INVITE (?P<call_id>.*)\] <script>: {INVITE_TAG} (?P<src>.*) -> (?P<dst>.*) via (.*)UA: (?P<user_agent>.*?),(.*)"

# OTHER

ALREADY_PROCESSED_FILES_FILE = f"{BASE_PATH}/ALREADY_PROCESSED_FILES.TXT"

YEAR = "2024"

###################
#### VARIABLES ####
###################

# DATA

userAgentsByExtension = {}
calls = set()

# INPUT FILES

oldKamailioLogFiles = glob.glob(f"{CURRENT_LOG_FILE}-*{GZIP_EXTENSION}")
currentKamailioLogFiles = glob.glob(CURRENT_LOG_FILE)

allKamailioLogFiles = oldKamailioLogFiles

#################
#### CLASSES ####
#################

# PLATFORM ENUM

class UserAgentPlatform(Enum):
    MACOS = "macos"
    WINDOWS = "windows"
    LINUX = "linux"
    ANDROID = "android"
    IOS = "ios"
    POLYCOM = "polycom"
    OBI = "obi"
    TRUNK = "trunk"
    UNKNOWN = "unknown"

class NumberType(Enum):
    CERNPHONE = "CERNphone"
    CERNMOBILE = "mobile"
    EXTERNAL = "external"

# REGISTER ENTRY

class RegisterEntry:
    def __init__(self, extension, userAgentPlatform, userAgentVersion, userAgentFull, timestamp):
        self.extension = extension
        self.platform = userAgentPlatform
        self.version = userAgentVersion
        self.user_agent = userAgentFull
        self.timestamp = timestamp

    def __repr__(self):
        return f"{self.extension} - {self.user_agent}"

    # needed for not having duplicate entries
    def __eq__(self, other):
        if isinstance(other, RegisterEntry):
            return (self.extension == other.extension) and (self.user_agent == other.user_agent)
        else:
            return False
    def __hash__(self):
        return hash(self.__repr__())

# INVITE ENTRY

class InviteEntry:
    def _check_number_type(self, num):
        type_extension = NumberType.EXTERNAL

        if (len(num) == 5 and num.startswith("6") or num.startswith("7") or num.startswith("8")) or num.startswith("+4122766") or num.startswith("+4122767") or num.startswith("+4122768") or num.startswith("004122766") or num.startswith("004122767")  or num.startswith("004122768") or num.startswith("4122766") or num.startswith("4122767") or num.startswith("4122768") or num.replace(".", "").isalpha():
            type_extension = NumberType.CERNPHONE
        elif (len(num) == 6 and num.startswith("16")) or num.startswith("+4175411") or num.startswith("004175411") or num.startswith("4175411"):
            type_extension = NumberType.CERNMOBILE

        return type_extension

    def __init__(self, callId, src, dst, userAgentPlatform, userAgentVersion, user_agent, timestamp):
        self.callId = callId
        self.src = src
        self.typeExtension_src = self._check_number_type(src)
        self.dst = dst
        self.typeExtension_dst = self._check_number_type(dst)
        self.platform = userAgentPlatform
        self.version = userAgentVersion
        self.user_agent = user_agent
        self.timestamp = timestamp

    def __repr__(self):
        return f"{self.callId}"

    # needed for not having duplicate entries
    def __eq__(self, other):
        if isinstance(other, InviteEntry):
            return self.callId == other.callId
        else:
            return False
    def __hash__(self):
        return hash(self.__repr__())

###########################
#### AUXILIARY METHODS ####
###########################

# LOGGING

def getLogger(module_name = "default", debug_level = logging.DEBUG):
    logger = logging.getLogger(module_name)
    if not logger.hasHandlers():
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # logging to stdout
        sh = logging.StreamHandler()
        sh.setLevel(debug_level)
        sh.setFormatter(formatter)

        logger.addHandler(sh)
        logger.setLevel(debug_level)
    return logger

logger = getLogger("cernphone_stats")

# PARSE USER AGENT

@lru_cache(maxsize=1000)
def parse_user_agent(userAgent):
    platform = UserAgentPlatform.UNKNOWN
    version = ""
    if userAgent.startswith("CERNphoneAndroid/") or userAgent.startswith("CERNphone/"):
        platform = UserAgentPlatform.ANDROID
        version = userAgent.split(" ")[0].split("/")[-1]
    elif userAgent.startswith("CERNphoneiOS/"):
        platform = UserAgentPlatform.IOS
        version = userAgent.split(" ")[0].split("/")[-1]
    elif userAgent.endswith("win32"):
        platform = UserAgentPlatform.WINDOWS
        version = userAgent.split(" ")[-1].split("-")[0]
    elif userAgent.endswith("macOS"):
        platform = UserAgentPlatform.MACOS
        version = userAgent.split(" ")[-1].split("-")[0]
    elif userAgent.endswith("linux"):
        platform = UserAgentPlatform.LINUX
        version = userAgent.split(" ")[-1].split("-")[0]
    elif userAgent.startswith("OBIHAI/"):
        platform = UserAgentPlatform.OBI
        version = userAgent.split("/")[-1].split("-")[0]
    elif userAgent.startswith("PolycomVVX") or userAgent.startswith("PolyEdge"):
        platform = UserAgentPlatform.POLYCOM
        model_split = userAgent.split("/")[0].split("-")
        version = model_split[1] if len(model_split) > 1 else ""
    elif userAgent.startswith("Poly/"):
        platform = UserAgentPlatform.POLYCOM
        version = userAgent.split("/")[-1].split("-")[0]
    elif userAgent == 'CERN SIP' or userAgent.startswith("Asterisk PBX"):
        platform = UserAgentPlatform.TRUNK
    return platform, version

# CREATE USER AGENT ENTRY

def createRegisterEntry(extension, userAgentRaw, timestamp):
    platform, version = parse_user_agent(userAgentRaw)
    return RegisterEntry(extension, platform, version, userAgentRaw, timestamp)

# CREATE CALL ENTRY

def createInviteEntry(callId, src, dst, userAgentRaw, timestamp):
    platform, version = parse_user_agent(userAgentRaw)
    return InviteEntry(callId, src, dst, platform, version, userAgentRaw, timestamp)

# ADD USER AGENT TO THE LIST

def addUserAgent(extension, userAgentRaw, tsStr):
    if extension and userAgentRaw:
        timestamp = None
        if tsStr:
            try:
                timestamp = dateutil.parser.isoparse(tsStr)
            except RuntimeError:
                timestamp = datetime.strptime(tsStr, "%Y %b %d %H:%M:%S")
        else:
            timestamp = datetime.now()
        userAgentEntry = createRegisterEntry(extension, userAgentRaw, timestamp)
        userAgentList = userAgentsByExtension.get(extension)
        if not userAgentList:
            userAgentsByExtension[extension] = set()
        userAgentsByExtension[extension].add(userAgentEntry)

# ADD CALL TO THE LIST

def add_call(callId, src, dst, userAgentRaw, tsStr):
    if callId and src and userAgentRaw:
        timestamp = None
        if tsStr:
            try:
                timestamp = dateutil.parser.isoparse(tsStr)
            except RuntimeError:
                timestamp = datetime.strptime(tsStr, "%Y %b %d %H:%M:%S")
        else:
            timestamp = datetime.now()
        call_entry = createInviteEntry(callId, src, dst, userAgentRaw, timestamp)
        calls.add(call_entry)

# CHECK IF FILE WAS ALREADY PROCESSED

def file_was_processed(filename):
    with open(ALREADY_PROCESSED_FILES_FILE, "rb", 0) as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
            return s.find((filename + "\n").encode()) != -1

# ADD FILE TO ALREADY PROCESSED LIST

def record_processed_file(filename):
    with open(ALREADY_PROCESSED_FILES_FILE, "a+", encoding="utf-8") as f:
        f.write(filename + "\n")

# PROCESS INDIVIDUAL LINE

def processLine(line):
    if INVITE_TAG in line:
        match = re.search(INVITE_PATTERN, line, flags=0)
        if match:
            date_tmp = line.split(" ", 4)
            if date_tmp[1] != "":
                date_timestamp_str = YEAR+" "+date_tmp[0]+" "+date_tmp[1]+" "+date_tmp[2]
            else:
                date_timestamp_str = YEAR+" "+date_tmp[0]+" "+date_tmp[2]+" "+date_tmp[3]
            src = match.group("src")
            dst = match.group("dst")
            userAgentRaw = match.group("user_agent")
            callId = match.group("call_id")
            add_call(callId, src, dst, userAgentRaw, date_timestamp_str)
    elif REGISTER_TAG in line:
        match = re.search(REGISTER_PATTERN, line, flags=0)
        if match:
            date_tmp = line.split(" ", 4)
            if date_tmp[1] != "":
                date_timestamp_str = YEAR+" "+date_tmp[0]+" "+date_tmp[1]+" "+date_tmp[2]
            else:
                date_timestamp_str = YEAR+" "+date_tmp[0]+" "+date_tmp[2]+" "+date_tmp[3]
            extension = match.group("extension")
            userAgentRaw = match.group("user_agent")
            addUserAgent(extension, userAgentRaw, date_timestamp_str)

# PROCESS WHOLE FILE

def process_file(filename):
    YEAR = re.search("20[0-4][0-9]",filename).group()

    if file_was_processed(filename):
        logger.debug("Ignoring already processed file %s ...", filename)
        return
    logger.debug("Processing %s...", filename)
    try:
        if GZIP_EXTENSION in filename:
            file = gzip.open(filename, "rt", errors="ignore")
        else:
            file = open(filename, "r", errors="ignore", encoding="utf-8")
        for line in file:
            processLine(line)
        record_processed_file(filename)
        file.close()
    except Exception as e:
        logger.error("Error reading file: %s. Exception %s.", filename, e)

# FORMAT DATA
def format_data():
    """returns both register and invite lists."""
    data_invite = []
    data_register = []
    # Registers
    for userAgents in userAgentsByExtension.values():
        data_register += [(userAgent.timestamp, str(userAgent.extension), userAgent.platform.value, userAgent.version, userAgent.user_agent) for userAgent in userAgents]
    # Invites
    data_invite += [(call.timestamp, call.src, call.dst, call.callId, call.platform.value, call.version, call.typeExtension_src.value, call.typeExtension_dst.value, call.user_agent) for call in calls]
    return data_register, data_invite

# STORE DATA
def send_register(postgrecursor, data):
    """this function is inserting (sending) the data to the target database"""
    sql = """
    INSERT INTO register(time, extension, client_platform, client_version, user_agent)
    VALUES( %s, %s, %s, %s, %s)"""
    postgrecursor.executemany(sql, data)
    postgreconn.commit()

def send_invite(postgrecursor, data):
    """this function is inserting (sending) the data to the target database"""
    sql = """
    INSERT INTO invite(time, source, destination, call_id, client_platform, client_version, source_type, destination_type, user_agent)
    VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    postgrecursor.executemany(sql, data)
    postgreconn.commit()


##############
#### MAIN ####
##############

if __name__ == "__main__":
    # CHECK IF THERE'RE FILES TO PROCESS

    if not allKamailioLogFiles:
        logger.warning("No files to process")
        sys.exit(1)

    # PROCESS LOG FILES

    for logFile in allKamailioLogFiles:
        process_file(logFile)

    # PARSE AND SEND DATA

    data, data2 = format_data()

    with postgreconn.cursor() as postgrecursor:
        send_register(postgrecursor, data)
        send_invite(postgrecursor, data2)
    #logger.info(f"Sent {len(data)} + {len(data2)} entries!")
    logger.info("Sent %s + %s entries!", len(data), len(data2))

    # DELETE ALL PROCESSED FILES

    for logFile in allKamailioLogFiles:
        os.remove(logFile)
