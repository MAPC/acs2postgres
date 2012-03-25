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
        
    def createTable(self, tableName, pk_head, unique_head, other_head):
        """
        pk_head should be the form pk_name and is the primary key 
        column. If None, an id field is automatically created. The type should either be
        'serial' or 'integer'. DO NOT PK OFF OF A STRING.
        
        unique_head and other_head is of the form: (col name, type). Type can be any
        valid db type. 
        
        unique_head is a list of unique key values. Note that id can be assumed to exist,
        so long as pk_head contains id or is left blank.
        """
        if pk_head == None: pk_head = []
        if unique_head == None: unique_head = []
        if other_head == None: other_head = []
        #print tableName, pk_head, unique_head, other_head
        logging.info("Trying to drop table %s" % tableName)
        if self.execute("DROP TABLE %s CASCADE;" % tableName) == None:
            logging.error("Dropping table %s failed" % tableName)
        else:
            logging.info("dropped table %s" % tableName)
        
        cmd_str = "CREATE TABLE %s (" % tableName
        if len(pk_head) != 0:
            cmd_str = "%s %s %s primary key, " % (cmd_str, pk_head[0], pk_head[1])
        else:
            cmd_str = "%s id serial primary key, " % cmd_str
        
        for h, t in unique_head:
            cmd_str = "%s %s %s, " % (cmd_str, h, t)
        
        for h, t in other_head:
            cmd_str = "%s %s %s, " % (cmd_str, h, t)
        
        if len(unique_head) != 0:
            cmd_str = "%s UNIQUE( " % cmd_str
            for h, t in unique_head:
                cmd_str = "%s %s, " % (cmd_str, h)
            cmd_str = cmd_str.rstrip(",")
            cmd_str = "%s ) " % cmd_str
        cmd_str = cmd_str.rstrip(" ,") + (");")
        logging.info("Trying to create table: %s" % tableName)
        if self.execute(cmd_str) == None:
            logging.error("Failed to create table: %s" % tableName)
        else: 
            logging.info("Created table: %s" % tableName)
    
    def insert(self, table, col_names=[], data=[] ):
        """
        insert will run: insert into table (col_names) values data
        All of the data is wrapped in quotes and escaped. Postgeres is nice in that
        it allows numbers to be wrapped in quotes as still enter as a number if the column name.
        data needs to be in a matrix format with one entry for each column provided to col_names.
        """
        cmd_str = "INSERT INTO %s (" % table
        for col in col_names:
            cmd_str = "%s%s, " % (cmd_str, col)
        cmd_str = cmd_str.rstrip(", ") + ") VALUES "
        
        for row in data:
            cmd_str = "%s (" % cmd_str
            for col in row:
                cmd_str = "%s%s, " % (cmd_str, self.clean(col))
            cmd_str = cmd_str.rstrip(", ") + "),\n"
        cmd_str =  cmd_str.rstrip(",\n") + ";" 
        
        logging.debug("Running insert cmd")
        if self.execute(cmd_str) == None:
            logging.error("Insert command failed")
        logging.debug("Insert cmd succeeded")
    
    def colTypeFromArray(self, line=[], headerType={}):
        """
        This will read each array object and try to determine the type.
        headerTypes is used to compare the current guess for the cell. Initially
        pass in an empty dictionary and on subsequent passes pass back the returned dictionary. 
        If the length of the array object is zero, the type can not be determined
        so "null" will be returned.
        If the guess is currently float, but float fails to be parsed, convert to null 
        (this should rarely if ever happen)
        If guess is currently int but float passes, make the column a float
        If int fails, make varchar(len(string))
        
        NOTE: for now only pass in non-zero length empty strings if working with
        column delineated data. This will not work with data if the first iteration
        the cell is one length and subsequent iterations have the cell a different length 
        """
        x = 0
        for cell in line:
            #print "X: %d ; Cell: %s ; Line[x]: %s" % (x, cell, line[x])
            if x in headerType.keys():
                #if x == 45 or x == 44:
                #    print x, cell, line[x], headerType[x]
                
                if len(cell) == 0:
                    x = x + 1
                    #no information given about the data type, should assume string
                    continue
                else:
                    ret = None
                    try:
                        #if passes an int test and the field is currently null make the field an int
                        ret = int(cell)
                        if headerType[x] == "null":
                            headerType[x] = "integer"
                        #if it's an integer don't change it, if it's a float don't change it
                    except ValueError:
                        ret = None
                    
                    if ret == None:
                        try:
                            #if passes float but the curent deffinition is not a string (int or null only)
                            # change the type to a float
                            ret = float(cell)
                            if headerType[x] == "integer" or headerType[x] == "null":
                                headerType[x] = "float" 
                        except ValueError:
                            ret = None
                    if ret == None:
                        if headerType[x] == "integer" or headerType[x] == "float" or headerType[x] == "null": 
                            if len(cell.strip()) != 0:
                                headerType[x] = "varchar(%s)" % len(cell)
                        elif headerType[x].find("varchar") != -1:
                            cur_length = headerType[x].strip(")").split("varchar(")[1]
                            cur_length = int(cur_length)
                            if len(cell) > cur_length:
                                #print "Before: %s" % headerType[x]
                                headerType[x] = "varchar(%s)" % len(cell)
                                #print "After %s" % headerType[x]
            else:
                if len(cell) == 0:
                    headerType[x] = "null"
                else:
                    ret = None
                    try:
                        ret = int(cell)
                        headerType[x] = "integer"
                    except ValueError:
                        ret = None
                    
                    if ret == None:
                        try:
                            ret = float(cell)
                            headerType[x] = "float"
                        except ValueError:
                            ret = None
                    
                    if ret == None:
                        if len(cell.strip()) == 0:
                            headerType[x] = "null" #empty doesn't really give you a type, hope that it will eventually
                        else:
                            headerType[x] = "varchar(%s)" % len(cell)
            x = x + 1
        return headerType
    
        