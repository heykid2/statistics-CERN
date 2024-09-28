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

DEFAULT_DATE = "2023-05-01 00:00:00.000"

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

def get_last_date():
    """this function aims at retrieving the last recorded date in the target database"""
    postgrecursor.execute("SELECT START_TIME FROM history_agents ORDER BY START_TIME DESC LIMIT 1")
    results = postgrecursor.fetchone()
    if not results:
        date = DEFAULT_DATE
    else:
        date = results[0]
    return date

def get_last_run_callcenter(cc):
    """this function aims at retrieving the last recorded date in the callcenters specific databases"""
    send_query = f"SELECT * FROM status_snapshots_{cc} ORDER BY time DESC LIMIT 1"
    postgrecursor.execute(send_query)
    results = postgrecursor.fetchone()
    if not results:
        date = DEFAULT_DATE
    else:
        for row in range(2 , len(results)):
            dict_agents[list_agents[row-2][0]] = f"{results[row]}"
        date = results[1]
    return date

def get_next_state(event):
    """this fuction avoids code duplication"""
    if event.startswith("P"):
        return "PAUSED"
    elif event.startswith("U") or event.startswith("A") or event.startswith("COMPLE"):
        return "AVAILABLE"
    elif event == "CONNECT":
        return "ON_CALL"
    elif event.startswith("REMO"):
        return "DISCONNECTED"
    elif event == "TRANSFER":
        return "AVAILABLE"
    else:
        return None

def create_obj(entry):
    """this function creates an instance of the CallCenterNumber class"""
    tmp_agent = get_agent(entry[2])

    if (entry[2].startswith("L") or entry[2].startswith("7")) and entry[1] == 'NONE':
        tmp = tmp_agent.split("_", 1)
        tmp_queue = tmp[0]
    else:
        tmp_queue = entry[1]

    tmp_state = get_next_state(entry[3])
    return CallcenterNumber(tmp_agent, tmp_queue, tmp_state)

def modify_obj(entry, data_list):
    """this function modifies an existing object of the CallCenterNumber class
    when there is a change of state it is recorded"""
    obj_name = get_agent(entry[2])
    retrieved_object = variable_dict[obj_name]

    new_state = get_next_state(entry[3])

    if new_state != retrieved_object.state and retrieved_object.queue != 'NONE':
        retrieved_object.state = new_state
        for value in variable_dict.values():
            tmp_tuple = (value.agent, value.queue, value.state, entry[0])
            data_list.append(tmp_tuple)

def send_data(data_list):
    """this function is inserting (sending) the data to the target database."""
    sql = "INSERT INTO history_agents( AGENT, QUEUE, STATE, START_TIME) VALUES(%s, %s, %s, %s)"
    postgrecursor.executemany(sql, data_list)
    postgreconn.commit()

def get_list_agents(cc):
    """returns the list of agents."""
    if cc:
        query_agent = f"SELECT DISTINCT(agent) FROM history_agents WHERE queue = '{cc}';"
    else:
        query_agent = "SELECT DISTINCT(agent) FROM history_agents;"
    postgrecursor.execute(query_agent)
    list_agents = postgrecursor.fetchall()
    return list_agents

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
        where table_name = 'status_snapshots_{callcenter}';
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
            print(agent_to_be_created)
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

def create_tuple_callcenter(dict_agents, list_agents, time):
    """return a tuple containing each agents status and the curent date (string)."""
    res = (time.strftime("%Y-%m-%d %H:%M:%S.%f"),)
    for agent_from_list in list_agents:
        res += (dict_agents[agent_from_list[0]],)
    return res

def send_data_callcenter(data_list, list_agents, cc):
    """this function is inserting (sending) the data to the target database."""
    sql = f"INSERT INTO status_snapshots_{cc}( TIME, "
    for i in range(len(list_agents) - 1):
        sql += f"A{list_agents[i][0]}, "
    sql += f"A{list_agents[len(list_agents) -1][0]}) VALUES("
    for i in range(len(list_agents)):
        sql += "%s,"
    sql += " %s)"
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
            variable_dict = {}
            postgrecursor.execute("""
                CREATE TABLE IF NOT EXISTS history_agents(
                ID SERIAL PRIMARY KEY NOT NULL,
                AGENT VARCHAR (20) NOT NULL,
                QUEUE INT,
                STATE VARCHAR (20),
                START_TIME TIMESTAMP);
                """)
            lastTime = get_last_date()
            if lastTime != DEFAULT_DATE:
                list_agents = get_list_agents(None)
                for agent in list_agents:
                    query = f"""
                        SELECT state, queue, start_time 
                        FROM history_agents 
                        WHERE agent = '{agent[0]}' 
                        ORDER BY start_time DESC LIMIT 1
                        """
                    postgrecursor.execute(query)
                    last_state = postgrecursor.fetchone()
                    if last_state:
                        obj_value = CallcenterNumber(agent[0], last_state[1], last_state[0])
                        variable_dict[agent[0]] = obj_value
            SQL = """
                SELECT TIME, QUEUENAME, AGENT, EVENT FROM CALLCENTER_PROD.QUEUE_LOG_NODE_MAIN 
                WHERE EVENT IN 
                    ('ADDMEMBER','PAUSE','PAUSEALL','REMOVEMEMBER','UNPAUSE','UNPAUSEALL', 'CONNECT', 'COMPLETEAGENT', 'COMPLETECALLER', 'ATTENDEDTRANSFER', 'BLINDTRANSFER') 
                    AND TIME > :1
                UNION 
                SELECT TIME, QUEUENAME, AGENT, EVENT FROM CALLCENTER_PROD.QUEUE_LOG_NODE_BACKUP 
                WHERE EVENT IN 
                    ('ADDMEMBER','PAUSE','PAUSEALL','REMOVEMEMBER','UNPAUSE','UNPAUSEALL', 'CONNECT', 'COMPLETEAGENT', 'COMPLETECALLER', 'ATTENDEDTRANSFER', 'BLINDTRANSFER') 
                    AND TIME > :2
                ORDER BY TIME ASC
                """
            data = []
            for row in cursor.execute(SQL, (str(lastTime), str(lastTime))):
                if len(data) > 2000:
                    send_data(data)
                    data = []
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
            for callcenter in callcenter_list:
                list_agents = get_list_agents(callcenter)
                create_table_callcenter(callcenter, list_agents)
                dict_agents = create_dict_callcenter(list_agents)
                lastTime_cc = get_last_run_callcenter(callcenter)
                query = """
                    SELECT agent, state, start_time 
                    FROM history_agents 
                    WHERE start_time > %s AND queue = %s
                    ORDER BY start_time asc;"""
                postgrecursor.execute(query, (lastTime, callcenter))
                results = postgrecursor.fetchall()
                data = []
                rectime = None
                for row in results:
                    if len(data) > 2000:
                        send_data_callcenter(data, list_agents, callcenter)
                        data = []

                    if not rectime:
                        rectime = row[2]
                    elif rectime and rectime != row[2]:
                        data.append(create_tuple_callcenter(dict_agents, list_agents, rectime))
                        rectime = row[2]
                    dict_agents[row[0]] = row[1]
                if rectime:
                    data.append(create_tuple_callcenter(dict_agents, list_agents, rectime))
                send_data_callcenter(data, list_agents, callcenter)
            #third part:
            #Creates a timeline which contains the number of agents available and the number of
            #agents connected.
            postgrecursor.execute("""CREATE TABLE IF NOT EXISTS available_connected(
                ID SERIAL PRIMARY KEY NOT NULL,
                TIME TIMESTAMP,
                CALLCENTER VARCHAR (20) NOT NULL,
                AVAILABLE INT,
                CONNECTED INT);
                """)
            available = 0
            connected = 0
            for callcenter in callcenter_list:
                SQL = """
                    SELECT start_time, state 
                    FROM history_agents 
                    WHERE queue = %s and start_time > %s
                    ORDER BY start_time asc
                    """
                postgrecursor.execute(SQL, (callcenter, lastTime))
                results = postgrecursor.fetchall()
                time = None
                for row in results:
                    if row[0] != time:
                        if time:
                            av_con_tmp = (row[0], callcenter, available, connected)
                            av_con_data.append(av_con_tmp)
                        time = row[0]
                        available = 0
                        connected = 0
                    if row[1]:
                        if row[1].startswith("A"):
                            available += 1
                            connected += 1
                        elif row[1].startswith("P"):
                            connected += 1
                        elif row[1].startswith("O"):
                            connected += 1
                        if len(av_con_data) > 2000:
                            send_data_av_con(av_con_data)
                            av_con_data = []
                send_data_av_con(av_con_data)

postgreconn.close()
