"""imap-oauth2 Entrypoint."""
import time

import psycopg2

from oauth2_imap.config import load_config
from oauth2_imap.imap_client import ImapClient

#postgreSQL CONFIG
postgreconn = psycopg2.connect(database="xxxxxxxx",
                        host="xxxxxxxxxxxxxxxx",
                        user="xxxxxxxxxxxxxxxxxx",
                        password="xxxxxxxxxxxxxxxxxxxxxxx",
                        port="xxxx")

if __name__ == "__main__":
    with postgreconn.cursor() as postgrecursor:
        conf = load_config()
        z = ImapClient(conf)
        while True:
            time.sleep(60)
            z.process_mailbox(postgreconn, postgrecursor)
postgreconn.close()
