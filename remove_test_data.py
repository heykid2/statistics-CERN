import psycopg2

#postgreSQL CONFIG
postgreconn = psycopg2.connect(
    database=os.getenv("POSTGRESQL_DATABASE", "xxxxxxxx"),
    host=os.getenv("POSTGRESQL_HOSTNAME", "xxxxxxxx"),
    user=os.getenv("POSTGRESQL_USERNAME", "xxxxxxxx"),
    password=os.getenv("POSTGRESQL_PASSWORD"),
    port=os.getenv("POSTGRESQL_PORT", "xxxxxxxx")
)

#############
## TO KEEP ##
#############

list_cc = ['74444', '74848', '77777', '75555', '76666', '66666', '72201']


with postgreconn.cursor() as postgrecursor:
    #recreated_calls
    sql = """
        DELETE FROM 
            recreated_calls 
        WHERE
            CALLDATE < '2024-02-20 00:00:00.000'
    """
    postgrecursor.execute(sql)
    postgreconn.commit()
    #available_connected
    sql = """
        DELETE FROM 
            available_connected 
        WHERE
            time < '2024-02-20 00:00:00.000'
    """
    postgrecursor.execute(sql)
    postgreconn.commit()
    #history_agents
    sql = """
        DELETE FROM 
            history_agents 
        WHERE
            start_time < '2024-02-20 00:00:00.000'
    """
    postgrecursor.execute(sql)
    postgreconn.commit()
    #status_snapshots
    for cc in list_cc:
        sql = f"""
            DELETE FROM 
                status_snapshots_{cc[1]} 
            WHERE
                time < '2024-02-20 00:00:00.000'
        """
        postgrecursor.execute(sql)
        postgreconn.commit()
