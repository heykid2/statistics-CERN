"""Process incoming emails."""
import base64
import imaplib
from html.parser import HTMLParser
import re
from datetime import datetime

from imap_tools import MailBox
from oauth2_imap.config import Config
from oauth2_imap.oauth2_flow import Oauth2Flow

class MyHTMLParser(HTMLParser):
    """Parsing the mails"""
    building = ""
    start_date = ""
    end_date = ""
    site = ""

    def handle_data(self, data):
        if data: 
            if re.search("CERN", data) or re.search(":", data):
                if re.search("CERN", data):
                    self.building = data
                elif re.search("20", data):
                    if self.start_date == "":
                        self.start_date = datetime.strptime(data, '%d.%m.%Y %H:%M')
                    else:
                        self.end_date = datetime.strptime(data, '%d.%m.%Y %H:%M')
            elif re.search("C", data) and len(data) == 4:
                self.site = data

def send_data(postgreconn, postgrecursor, data):
    """this function is inserting (sending) the data to the target database"""
    query = """INSERT INTO swisscom_mobile_interruption (BUILDING, SITE, START_DATE, END_DATE) 
        VALUES(%s, %s, %s, %s)"""
    postgrecursor.execute(query, data)
    postgreconn.commit()

def send_data_timeline(postgreconn, postgrecursor, data):
    """this function is inserting (sending) the data to the target database"""
    query = "INSERT INTO swisscom_service_timeline (DATE, AVAILABILITY) VALUES(%s, %s)"
    postgrecursor.executemany(query, data)
    postgreconn.commit()

def update_data(postgreconn, postgrecursor, start_date, end_date, building):
    """this function is inserting (sending) the data to the target database"""
    query = """UPDATE swisscom_mobile_interruption SET END_DATE = %s 
        WHERE START_DATE = %s and building = %s"""
    postgrecursor.execute(query, [end_date, start_date, building])
    postgreconn.commit()

class ImapClient:
    """Process incoming emails."""
    def __init__(self, conf: Config) -> None:
        """Initialize the Imap client."""
        self.conf = conf
        self.oauth = Oauth2Flow(conf)

    def process_mailbox(self, postgreconn, postgrecursor) -> None:
        """Process mailbox imap access"""
        print("Fetching emails...")
        self.__process_bulk(postgreconn, postgrecursor)

    def __process_bulk(self, postgreconn, postgrecursor) -> None:
        """Process bulk with imap-tools library"""
        # Authenticate to account using OAuth 2.0 mechanism
        timeline_data = []
        access_token, username = self.oauth.get_access_token()
        with MailBox(self.conf.IMAP_SERVER).xoauth2(
            username,
            access_token, # sasl xoauth2 token is built in the library
            "INBOX",
        ) as mailbox:
            for msg in mailbox.fetch(limit=5):
                if (re.search("unplanned interruption" , msg.subject)):
                    text = "<!DOCTYPE html>" + msg.html
                    parser = MyHTMLParser()
                    parser.feed(text)
                    if (re.search("Start" , msg.subject)):
                        tmp = (parser.building, parser.site, parser.start_date, None)
                        send_data(postgreconn, postgrecursor, tmp)
                    elif (re.search("End" , msg.subject)):
                        #update
                        timeline_data = [(parser.start_date, 0), (parser.end_date, 1)]
                        send_data_timeline(postgreconn, postgrecursor, timeline_data)
                        update_data(postgreconn, postgrecursor, parser.start_date, parser.end_date, parser.building)
                    if mailbox.folder.exists('processed'):
                        mailbox.move([msg.uid], 'processed')
                    else:
                        mailbox.folder.create('processed')
                else:
                    if mailbox.folder.exists('maintenance'):
                        mailbox.move([msg.uid], 'maintenance')
                    else:
                        mailbox.folder.create('maintenance')

    def __process_standard(self) -> None:
        """Process normal imaplib authentication"""
        # Authenticate to account using OAuth 2.0 mechanism
        access_token, username = self.oauth.get_access_token()
        # Apparently 365 wants clear data, not base64, so set to False
        auth_string = self.sasl_xoauth2(username, access_token, False)
        imap_conn = imaplib.IMAP4_SSL(self.conf.IMAP_SERVER)
        imap_conn.debug = 4
        imap_conn.authenticate('XOAUTH2', lambda x: auth_string)
        imap_conn.select('INBOX', readonly=True)
        imap_conn.close()

    def sasl_xoauth2(self, username, access_token, base64_encode=False) -> str:
        """Convert the access_token into XOAUTH2 format"""
        auth_string = "user=%s\1auth=Bearer %s\1\1" % (username, access_token)
        if base64_encode:
            auth_string = base64.b64encode(auth_string.encode("ascii")).decode("ascii")
        return auth_string
