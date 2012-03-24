import openpyxl
import os, sys
import logging
import psycopg2
from psycopg2 import ProgrammingError

FILE="/home/ben/workspace/MAPC_DevtDatabase_V1_3_21_11.xlsx"


def colTypeFromArray(line=[], headerType={}):
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

def clean(text):
    """
    This takes a piece of text and cleans it for input into the
    database.
    """
    if len(text) == 0:
        return "NULL"
    elif (text.strip() == '.'):
        return "E'0'" #In the CSV files a 0 is represented by a '.', it should be 0
    else:
        text = text.replace("%", "")
        text = text.replace("'", "\\'")
        text = text.replace(",", "\\,")
        return "E'%s'" % text

def execute(cmd):
    """
    Execute the sql command
    """
    try:
        conn = psycopg2.connect(host="localhost", database="development", user="ben", password="password")
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
    
def insert(table, col_names=[], data=[] ):
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
                cmd_str = "%s%s, " % (cmd_str, clean(col))
            cmd_str = cmd_str.rstrip(", ") + "),\n"
        cmd_str =  cmd_str.rstrip(",\n") + ";" 
        
        logging.debug("Running insert cmd")
        if execute(cmd_str) == None:
            logging.error("Insert command failed")
        logging.debug("Insert cmd succeeded")    
    
def createTable(tableName, pk_head, unique_head, other_head):
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
    if execute("DROP TABLE %s CASCADE;" % tableName) == None:
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
    if execute(cmd_str) == None:
        logging.error("Failed to create table: %s" % tableName)
    else: 
        logging.info("Created table: %s" % tableName)


def setupLog(logFile, verbose):
    fmt="%(asctime)s %(levelname)s: %(message)s"
    level = None
    if verbose == 0:
        level = logging.ERROR
    elif verbose == 1:
        level = logging.WARNING
    elif verbose == 2:
        level = logging.INFO
    elif verbose >= 3:
        level = logging.DEBUG
    logging.basicConfig(filename=logFile, format=fmt, level=level)



setupLog("./db_from_xls.log", 3)
book = openpyxl.load_workbook(FILE)
sheet = book.get_sheet_by_name("MAPC_DevtData_V1_3_21_11")
firstRow = True
headers = []
data = []
header_dict = {}
for row in sheet.rows:
    data_row = []
    for cell in row:
        if firstRow == True:
            headers.append(cell.value)
        else:
            value = ""
            if cell.value != None:
                value = "%s" % cell.value
            data_row.append(value)
    if firstRow == True:
        firstRow = False
    else:
        #print data_row[45]
        header_dict = colTypeFromArray(data_row, header_dict)
        data.append(data_row)

db_headers = []
for x in range(0, len(headers)):
    db_headers.append((headers[x], header_dict[x]))
#print header_dict
createTable("migrate_table", db_headers[0], None, db_headers[1:])
insert("migrate_table", headers, data)

    
    

