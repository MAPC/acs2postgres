import psycopg2
from psycopg2 import ProgrammingError
import logging

class DBOps():
    def __init__(self, db_host, db_port, db_database, db_user, db_pass):
        self.db_host = db_host
        self.db_port = db_port
        self.db_database = db_database
        self.db_user = db_user
        self.db_pass = db_pass
    
    def execute(self, cmd):
        """
        Execute the sql command
        """
        try:
            conn = psycopg2.connect(host=self.db_host, database=self.db_database, user=self.db_user, password=self.db_pass)
            cur = conn.cursor()
            cur.execute(cmd)
            conn.commit()
        except Exception, e:
            logging.error("A database operation failed. The error is: %s. \nThe command is: %s" % (e,cmd))
            return None
        
        try:
            ret = cur.fetchall()
            return ret
        except ProgrammingError:
            return []
        conn.close()
        
        