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

callcenter_list = ('77777', '66666', '72201', '74444', '75555', '74848', '76666')

list_cc = [('2023-11-27 00:00:00.000', '77777', '2024-02-20 00:00:00.000')]

list_agents = [('67341',),('61230',), ('61229',), ('67258',), ('61783',), ('67107',), ('69019',),\
    ('67539',), ('62810',), ('67852',), ('60036',), ('61104',), ('67633',), ('67226',)]

#################
#### CLASSES ####
#################

class CallcenterNumber:
    """A way to store the evolution of the STATE of an agent"""
    def __init__(self, agent, queue, state):
        self.agent = agent
        self.queue = queue
        self.state = state

###########################
#### AUXILIARY METHODS ####
###########################

def get_agent(entry_string):
    '''
    in the original database the agent could be in the wrong format like "Local/76666@agents/n"
    this function returns the agent in the right format
    '''
    if entry_string[0].startswith("L"):
        tmp = entry_string.split("/",1)
        tmp2 = tmp[-1].split("@",1)
        agent = tmp2[0]
    else:
        agent = entry_string
    return agent

def create_obj(entry):
    """this function creates an instance of the CallCenterNumber class"""
    tmp_agent = get_agent(entry[2])

    if (entry[2].startswith("L") or entry[2].startswith("7")) and entry[1] == 'NONE':
        tmp = tmp_agent.split("_", 1)
        tmp_queue = tmp[0]
    else:
        tmp_queue = entry[1]
    tmp_state = ''
    if entry[3].startswith("P"):
        tmp_state = "PAUSED"
    elif entry[3].startswith("U") or entry[3].startswith("ADD") or entry[3].startswith("COMPLET"):
        tmp_state = "AVAILABLE"
    elif entry[3].startswith("CONN"):
        tmp_state = "ON_CALL"
    elif entry[3].startswith("REMO"):
        tmp_state = "DISCONNECTED"
    return CallcenterNumber(tmp_agent, tmp_queue, tmp_state)

def modify_obj(entry, data_list):
    """this function modifies an existing object of the CallCenterNumber class
    when there is a change of state it is recorded"""
    change_state = True
    obj_name = get_agent(entry[2])
    retrieved_object = variable_dict[obj_name]
    if entry[3].startswith("P") and not retrieved_object.state == "PAUSED":
        retrieved_object.state = "PAUSED"
    elif (entry[3].startswith("U") or entry[3].startswith("A") or entry[3].startswith("COMPLE")) and retrieved_object.state != "AVAILABLE":
        retrieved_object.state = "AVAILABLE"
    elif entry[3].startswith("CONN") and not retrieved_object.state == "ON_CALL":
        retrieved_object.state = "ON_CALL"
    elif entry[3].startswith("REMO") and not retrieved_object.state == "DISCONNECTED":
        retrieved_object.state = "DISCONNECTED"
    elif entry[3].endswith("TRANSFER") and retrieved_object.state != "AVAILABLE":
        retrieved_object.state = "AVAILABLE"
    else:
        change_state = False
    if change_state and retrieved_object.queue != 'NONE':
        for values in variable_dict.items():
            tmp_tuple = (values[1].agent, values[1].queue, values[1].state, entry[0])
            data_list.append(tmp_tuple)

def send_data(data_list):
    """this function is inserting (sending) the data to the target database."""
    sql = "INSERT INTO history_agents( AGENT, QUEUE, STATE, START_TIME) VALUES(%s, %s, %s, %s)"
    postgrecursor.executemany(sql, data_list)
    postgreconn.commit()

def create_table_callcenter(cc, list_agents):
    """this function creates a table per callcententer with the appropriate size."""
    send_query = f"""
        CREATE TABLE IF NOT EXISTS status_snapshots_{cc}(
        ID SERIAL PRIMARY KEY NOT NULL,
        TIME TIMESTAMP,
        """
    for agent_from_list in range(len(list_agents) - 1):
        send_query += f"A{list_agents[agent_from_list][0]} VARCHAR (20),"
    send_query += f"A{list_agents[len(list_agents)-1][0]} VARCHAR (20));"
    postgrecursor.execute(send_query)
    postgreconn.commit()

    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        where table_name = 'status_snapshots_{cc}';
        """
    postgrecursor.execute(sql)
    list_columns_raw = postgrecursor.fetchall()
    list_columns = []
    for column in list_columns_raw:
        list_columns.append(column[0])

    number_rows = len(list_columns) - 2
    if number_rows < len(list_agents):
        new_agents = []
        for element in list_agents:
            element = "a" + element[0]
            if element not in list_columns:
                new_agents.append(element)
        for agent_to_be_created in new_agents:
            sql = f"""
                ALTER TABLE IF EXISTS status_snapshots_{callcenter} 
                ADD COLUMN {agent_to_be_created} VARCHAR(20);
                """
            postgrecursor.execute(sql)
            postgreconn.commit()

def create_dict_callcenter(list_agents):
    """returns a dictionary containing every agents as a key and the status as its value."""
    dic_agents = {}
    for agent in enumerate(list_agents):
        tmp = agent[1][0]
        dic_agents[tmp] = ''
    return dic_agents

def create_tuple_callcenter(dict_agents, time):
    """return a tuple containing each agents status and the curent date (string)."""
    res = (time.strftime("%Y-%m-%d %H:%M:%S.%f"),)
    for agent_from_dict in dict_agents:
        res += (dict_agents[agent_from_dict],)
    return res

def send_data_callcenter(data_list, list_agents, cc):
    """this function is inserting (sending) the data to the target database."""
    length = len(list_agents)
    sql = f"INSERT INTO status_snapshots_{cc[1]}( TIME, "
    for i in range(length - 1):
        sql += f"a{list_agents[i][0]}, "
    sql += f"a{list_agents[length - 1][0]}) VALUES("
    for i in range(length):
        sql += "%s,"
    sql += "%s)"
    postgrecursor.executemany(sql, data_list)
    postgreconn.commit()

def send_data_av_con(data_list):
    """this function is inserting (sending) the data to the target database."""
    sql = """
        INSERT INTO available_connected( TIME, CALLCENTER, AVAILABLE, CONNECTED) 
        VALUES(%s, %s, %s, %s)
        """
    postgrecursor.executemany(sql, data_list)
    postgreconn.commit()

##############
#### MAIN ####
##############

with oracledb.connect(dsn=ORACLE_CONNECT) as connection:
    #first part:
    #it creates (if it doesn't exist) the target database then is querying the original database
    #to fetch the data the data is stored temporarily (200 rows) and then sent as a batch.
    with connection.cursor() as cursor:
        with postgreconn.cursor() as postgrecursor:
            for cc in list_cc:
                variable_dict = {}
                SQL = f"""
                    SELECT TIME, QUEUENAME, AGENT, EVENT FROM CALLCENTER_PROD.QUEUE_LOG_{cc[1]}_NODE_MAIN 
                    WHERE EVENT IN 
                        ('ADDMEMBER','PAUSE','PAUSEALL','REMOVEMEMBER','UNPAUSE','UNPAUSEALL', 'CONNECT', 'COMPLETEAGENT', 'COMPLETECALLER', 'ATTENDEDTRANSFER', 'BLINDTRANSFER') 
                        AND TIME > '{cc[0]}' AND TIME < '{cc[2]}'
                    UNION 
                    SELECT TIME, QUEUENAME, AGENT, EVENT FROM CALLCENTER_PROD.QUEUE_LOG_{cc[1]}_NODE_BACKUP 
                    WHERE EVENT IN 
                        ('ADDMEMBER','PAUSE','PAUSEALL','REMOVEMEMBER','UNPAUSE','UNPAUSEALL', 'CONNECT', 'COMPLETEAGENT', 'COMPLETECALLER', 'ATTENDEDTRANSFER', 'BLINDTRANSFER') 
                        AND TIME > '{cc[0]}' AND TIME < '{cc[2]}'
                    ORDER BY TIME ASC
                    """
                data = []
                av_con_data = []
                for row in cursor.execute(SQL):
                    if len(data) > 2000:
                        last_row = data[-1]
                        data.pop(-1)
                        send_data(data)
                        data = [last_row]
                    agent_tmp = get_agent(row[2])

                    if agent_tmp not in variable_dict:
                        if not (row[1]=='NONE' and row[2].startswith("6")):
                            obj_value = create_obj(row)
                            variable_dict[agent_tmp] = obj_value
                    else:
                        modify_obj(row, data)
                send_data(data)
            #second part:
            #creates a status timeline per callcenter.
            av_con_data = []
            for callcenter in list_cc:
                dict_agents = create_dict_callcenter(list_agents)
                query = f"""
                    SELECT agent, state, start_time 
                    FROM history_agents 
                    WHERE start_time > '{callcenter[0]}' AND start_time < '{callcenter[2]}' AND queue = '{callcenter[1]}' 
                    ORDER BY start_time asc;"""
                postgrecursor.execute(query)
                results = postgrecursor.fetchall()
                data = []
                RECTIME = None
                for row in results:
                    if len(data) > 2000:
                        send_data_callcenter(data, list_agents, callcenter)
                        data = []
                    if row[0] not in list_agents:
                        print("yes")
                    else:
                        dict_agents[row[0]] = f"{row[1]}"
                    if RECTIME != row[2] or not RECTIME:
                        RECTIME = row[2]
                        data.append(create_tuple_callcenter(dict_agents, RECTIME))
                send_data_callcenter(data, list_agents, callcenter)
            #third part:
            #Creates a timeline which contains the number of agents available and the number of
            #agents connected.
            AVAILABLE = 0
            CONNECTED = 0
            for callcenter in list_cc:
                SQL = f"""
                    SELECT start_time, state 
                    FROM history_agents 
                    WHERE queue = '{callcenter[1]}' and start_time > '{callcenter[0]}' AND start_time < '{callcenter[2]}'
                    ORDER BY start_time asc
                    """
                postgrecursor.execute(SQL)
                results = postgrecursor.fetchall()
                TIME = None
                for row in results:
                    if row[0] != TIME:
                        if TIME:
                            av_con_tmp = (row[0], callcenter[1], AVAILABLE, CONNECTED)
                            av_con_data.append(av_con_tmp)
                        TIME = row[0]
                        AVAILABLE = 0
                        CONNECTED = 0
                    if row[1].startswith("A"):
                        AVAILABLE += 1
                        CONNECTED += 1
                    elif row[1].startswith("P"):
                        CONNECTED += 1
                    elif row[1].startswith("O"):
                        CONNECTED += 1
                    if len(av_con_data) > 2000:
                        send_data_av_con(av_con_data)
                        av_con_data = []
                send_data_av_con(av_con_data)
                