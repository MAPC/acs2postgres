# data retrieved from ftp://ftp.census.gov/acs2010_5yr/summaryfile/
# The geo pdf is the ftp://ftp.census.gov/acs2010_5yr/summaryfile/ACS_2006-2010_SF_Tech_Doc.pdf
import sys, argparse
import psycopg2, re
from psycopg2 import ProgrammingError
import ConfigParser, os, logging
import xlrd
from string import Template

#Threading stuff to deal with downloading the zip files and unpacking them
import  Queue, threading, zipfile
from zipfile import ZipFile

"""This class unzips a file"""
class ThreadUnzip(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
    def run(self):
        #This is always true because the queue.get command will raise an exception if empty 
        while True:
            unzip_s = self.queue.get()
            logging.info("Unzipping %s" % unzip_s)
            
            #unzip into a directory with the zipfile name
            #format should be in /path/to/zip/zipfile.zip
            unzip_loc = unzip_s.split(".")[0]
            zf = ZipFile(unzip_s, "r")
            zf.extractall(unzip_loc)
            zf.close()
            logging.info("Finished unzipping %s" % unzip_s)
            self.queue.task_done()

class Files():
    #seq_file = None denotes a geo file
    #add an assertion here 
    def __init__(self, seq_file=None, geo_file=None):
        assert seq_file != None or geo_file != None
        self.seq_file = seq_file
        self.e_files = []
        self.m_files = []
        self.geo_file = geo_file
    
    @classmethod
    def createTupples(cls, dict, seq_folder, folders=[]):
        """This will take the seq_folder which should have SeqX.xls files
        folders can be any number of folders but each one has to have the file structure
        e*, m* and a single g* file. The list returned will be a list of
        Files objects. The 'files' object will be a list of tupples, (e*, m*),
        folder locations. Sequence files """
        seq_re = re.compile(r"Seq(?P<id>\d+)\.xls")
        e_re = re.compile(r"e(?P<year>\d+)%s%s(?P<id>\d+)\.txt" %
                          ("5", dict["geo_lower"].split(".")[0]))
        m_re = re.compile(r"m(?P<year>\d+)%s%s(?P<id>\d+)\.txt" %
                          ("5", dict["geo_lower"].split(".")[0]))
        g_re = re.compile(r"g(?P<year>\d+)%s%s\.txt" %
                          ("5", dict["geo_lower"].split(".")[0]))
        seqs = os.listdir(seq_folder)
        folder_dict = {}
        geo = []
        
        for seq in seqs:
            s = seq_re.match(seq)
            if s != None:
                id = int(s.groups("id")[0])
                folder_dict[id] = Files("%s/%s" % (seq_folder, seq))
        
        geo_files = []
        for f in folders:
            files = os.listdir(f)
            for file in files:
                e = e_re.match(file)
                m = m_re.match(file)
                g = g_re.match(file)
                
                if e != None:
                    id = int(e.groups()[1])/1000
                    folder_dict[id].e_files.append("%s/%s" % (f, file))
                elif m != None:
                    id = int(m.groups()[1])/1000
                    folder_dict[id].m_files.append("%s/%s" % (f, file))
                elif g != None:
                    #The geo files with the same name are EXACTLY identical, regardless of source. 
                    if file not in geo_files:
                        id = "geo%s" % len(geo_files)
                        folder_dict[id] = Files(seq_file=None, geo_file="%s/%s" % (f, file))
        return folder_dict
                        
class ThreadFiles(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.col_re = re.compile(r"(?P<table_name>\w+)_(?P<col_name>\d+)")
        
        """
        try :
            self.conn = psycopg2.connect(host="localhost", dbname="y2010", user="ben", password="password")
            self.cur = self.conn.cursor()
        except:
            logging.critical("There was a problem logging into the db")
            sys.exit(0)
        """
    
    def execute(self, cmd):
        try:
            conn = psycopg2.connect(host="localhost", database="y2010", user="ben", password="password")
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
    def clean(self, text):
        if len(text) == 0:
            return "NULL"
        elif (text.strip() == '.'):
            return "E'0'" #In the CSV files a 0 is represented by a '.', it should be 0
        else:
            text = text.replace("%", "\\%")
            text = text.replace("'", "\\'")
            return "E'%s'" % text
    
    def colName(self, text):
        try:
            ret = int(text)
            return ("_%s" % text)
        except ValueError:
            return(text)
    
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
    
    def createMetaTables(self, dict_cols):
        for key in dict_cols.keys():
            if (key == "all"): continue
            
            table = "%s_meta" % key
            self.createTable( table, None, None, [("header", "varchar(10)"), ("description", "varchar(255)")] )
            
            data = []
            for k in sorted(dict_cols[key].keys()):
                data.append([k, dict_cols[key][k][0]])
            self.insert(table, ["header", "description"], data)
    
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
            if x in headerType.keys():
                if len(cell) == 0:
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
                        #We should never get here, but in case we do make the type a varchar
                        #if the current guess is null, otherwise leave it alone
                        if headerType[x] == "integer" or headerType[x] == "float" or headerType[x] == "null": 
                            if len(cell.strip()) != 0:
                                headerType[x] = "varchar(%s)" % len(cell)
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
    
    def columnTypes(self, dict_cols, files):
        """
        This will go through each of the e files and try to decern which db type
        the column is suppose to be and return the headerType dictionary keyed
        to column index. 
        """
        headerType = {}
        for e_file in files.e_files:
            f = open(e_file, "r")
            line = f.readline()
            f.close()
            line = line.split(",")
            
            """
            This is a bit confusing. The hirerarcy needs to be float -> int -> string
            If in one file the column is an int but in another, that same column is a float,
            the  column needs to be a float. The first file will be read into the 'else' section
            and the subsequent files are in the "if" section. 
            
            The 'else' section will go through and best guess the data type. The 'if' section
            will change the datatype if the need arises. 
            
            """
            headerType = self.colTypeFromArray(line, headerType)
        return headerType
                
    
    def createTables(self, dict_cols, singleFile):
        #pk_logRecNo tells if the table in pk_logRecNo
        ok_head = None
        if "LOGRECNO" in dict_cols["all"].keys():
            ok_head = ("LOGRECNO", "integer")
        
        colTypes = self.columnTypes(dict_cols, singleFile)
        
        for key in dict_cols.keys():
            if (key == "all"): continue
            
            #create the e table
            table = "%s_e" % key
            headers = []
            for col in sorted(dict_cols[key].keys()): 
                colType = colTypes[dict_cols[key][col][1]]
                if colType == "null":
                    colType = "varchar(255)"
                headers.append((self.colName(col), colType))
                
            self.createTable( table, ok_head, None, headers )
            
            #create the m table
            table = "%s_m" % key
            headers = []
            for col in sorted(dict_cols[key].keys()): 
                colType = colTypes[dict_cols[key][col][1]]
                if colType == "null":
                    colType = "varchar(255)"
                headers.append((self.colName(col), colType))
            self.createTable( table, ok_head, None, headers )
    
    def parseGeoLine(self, dict_lookup, line, strip=True):
        """This will parase a single line of text using the dict_lookup.
        The dict_lookup should be a dictionary with keys being the start column
        of the associated column name. So of the form:
        dict_lookup[0] = "column name"
        This should always be 0 indexed.
        The return value is an array of values indexed from the dictionary keys
        "strip" will strip the parsed substring string. Turn off if trying to get
        the header lengths.
        """
        assert 0 in dict_lookup.keys()
        cur_idx = 0
        ret = []
        for idx in sorted(dict_lookup.keys()):
            if idx == 0:
                cur_idx = idx
            else:
                if strip == True:
                    ret.append(line[cur_idx:idx].strip())
                else:
                    ret.append(line[cur_idx:idx])
                cur_idx = idx
        ret.append(line[cur_idx:])
        return ret
    
    def lookupFromCols(self, dict_cols_item):
        headers = []
        dict_lookup = {}
        for header in dict_cols_item.keys():
            dict_lookup[dict_cols_item[header][1]] = header
        
        for key in sorted(dict_lookup.keys()):
            headers.append(dict_lookup[key])
        return (headers, dict_lookup)
    
    def createGeoTables(self, dict_cols, singleFile):
        dict_lookup = {}
        for tableName in dict_cols:
            headers, dict_lookup = self.lookupFromCols(dict_cols[tableName])
            
            #column types try to read in from file, if nothing, assume string of specified length but for now 255
            headerType = {}
            geo_f = open(singleFile.geo_file)
            for line in geo_f.readlines():
                line = self.parseGeoLine(dict_lookup, line, strip=False)
                headerType = self.colTypeFromArray(line, headerType)
            geo_f.close()
            

            pk_head = None
            if "LOGRECNO" in dict_cols[tableName].keys():
                pk_head = ("LOGRECNO", "integer")
            other_head = []

            for header_idx in sorted(headerType.keys()):
                if headers[header_idx] != "LOGRECNO":
                    if headerType[header_idx] == "null":
                        other_head.append((headers[header_idx], "varchar(255)"))
                    else:
                        other_head.append((headers[header_idx], headerType[header_idx]))
            self.createTable(tableName, pk_head, None, other_head)
    
    def batchGeoInsert(self, table, col_names, data):
        logging.debug("Pushing data")
        self.insert(table, col_names, data)
        logging.debug("Pushed data")
    
    def insertGeoDataFromFile(self, dict_cols, singleFile):
        data = []
        
        for tableName in dict_cols.keys():
            headers, dict_lookup = self.lookupFromCols(dict_cols[tableName])
            
            geo_f = open(singleFile.geo_file)
            
            counter = 1
            for line in geo_f.readlines():
                if counter%200 == 0:
                    self.batchGeoInsert(tableName, headers, data)
                    data = []
                line = self.parseGeoLine(dict_lookup, line)
                data.append(line)
                counter = counter + 1
            self.batchGeoInsert(tableName, headers, data)
                

    def batchInsert(self, data, table_suffix, dict_cols, add_logRecNo=True):
        logging.debug("Pushing data")
        for tableName in data.keys():
            col_names = []
            col_names.append("LOGRECNO") #TODO: somehow make this not a static value
            
            for col_name in sorted(dict_cols[tableName].keys()):
                col_names.append(self.colName(col_name))
            logging.info("Inserting data to table: %s_%s" % (tableName, table_suffix))
            self.insert("%s_%s" % (tableName, table_suffix), col_names, data[tableName] )
        logging.debug("Finishing pushing data")
            
    def insertDataFromFile(self, file_handle, table_suffix, dict_cols, col_seperators):
        data = {}
        counter = 1
        for e_line in file_handle.readlines():
            if counter%200 == 0:
                self.batchInsert(data, table_suffix, dict_cols)
                data = {}
            e_line = e_line.split(",")
            for table_name in dict_cols.keys():
                data_line = [e_line[col_seperators[0]]] #the 0 separator is assumed to be the pk index
                if table_name == "all": continue
                if table_name not in data.keys():
                    data[table_name] = []
                for col in sorted(dict_cols[table_name].keys()):
                    value, col_idx = dict_cols[table_name][col]
                    data_line.append(e_line[col_idx].strip())
                    
                logging.debug("table name %s: data_line length: %s, number of columns in table: %s" %
                              ( table_name, len(data_line), len(dict_cols[table_name].keys())+1 ))
                data[table_name].append(data_line)
            counter = counter + 1
        self.batchInsert(data, table_suffix, dict_cols)
        
    
    def insertTableData(self, dict_cols, singleFile, col_seperators):
        for x in range(len(singleFile.e_files)):
            e_file = open(singleFile.e_files[x])
            m_file = open(singleFile.m_files[x])
            self.insertDataFromFile(e_file, "e", dict_cols, col_seperators)
            self.insertDataFromFile(m_file, "m", dict_cols, col_seperators)
            e_file.close()
            m_file.close()
    
    def seqInsert(self, singleFile):
        book = xlrd.open_workbook(singleFile.seq_file)
        #There are always two sheats, E and M. They are exactly the same
        #print len(book.sheets())
        sheet = book.sheets()[0]
        dict_cols = {"all" : {}}
        
        #
        # Indexes of separators because there are multiple tables per sheet.
        # This is required for the csv files to match up the data in particular columns to the correct tables.
        #Ended up only caring about the first one which is expected to be LOGRECNO   
        col_seperators = [] 
        
        #the csv column index
        col_idx = 0
        for col in range(sheet.ncols):
            m = self.col_re.match(sheet.cell(0, col).value)
            
            if m == None:
                dict_cols["all"][sheet.cell(0, col).value] = (sheet.cell(1, col).value, col_idx)
            else:
                #Use the fact that if the table_name is not in the dictionary yet
                #maping csv and header information is non trivial. Multiple tables with a main
                # make this a pain. Try to only work with say, 2000k inserts at a time. 
                if m.group("table_name") not in dict_cols.keys():
                    dict_cols[m.group("table_name")] = {}
                    col_seperators.append(col_idx)
                dict_cols[m.group("table_name")][m.group("col_name")] = (sheet.cell(1, col).value, col_idx)
            col_idx = col_idx + 1
        
        #The first seperator is the last column in the all section of the seq file, zero indexed.
        #If looking at the SeqX.xls file, this is the first k column headers, then the table names start
        #it is very clear when looking the spread sheet.  
        col_seperators.insert(0, len(dict_cols["all"].keys())-1)

        self.createMetaTables(dict_cols)
        self.createTables(dict_cols, singleFile)
        self.insertTableData(dict_cols, singleFile, col_seperators)
    
    def geoInsert(self, singleFile):
        #geo dict will have the format geo_dict[column name] = (description, start column) as a tupple
        #note that the PDF assumes a 1 index, python uses 0 so starting position is 0. 
        #TODO (Extension): This might go into an IPL file. If this happens, add config value in the [others]
        #section  
        geo_dict = {}
        dict_cols = {}
        geoFile = singleFile.geo_file
        #RECORD CODES
        geo_dict["FILEID"] = ("Always equal to ACS Summary File identification", 0)
        geo_dict["STUSAB"] = ("State Postal Abbreviation", 6)
        geo_dict["SUMLEVEL"] = ("Summary Level", 8)
        geo_dict["COMPONENT"] = ("Geographic Component", 11)
        geo_dict["LOGRECNO"] = ("Logical Record Number", 13)
        
        #GEOGRAPHIC AREA CODES
        geo_dict["US"] = ("US", 20)
        geo_dict["REGION"] = ("Census Region", 21)
        geo_dict["DIVISION"] = ("Census Division", 22)
        geo_dict["STATECE"] = ("State (Census Code)", 23)
        geo_dict["STATE"] = ("State (FIPS Code)", 25)
        geo_dict["COUNTY"] = ("State (FIPS Code)", 28)
        geo_dict["COUSUB"] = ("County Subdivision (FIPS)", 30)
        geo_dict["PLACE"] = ("Place (FIPS Code)", 35)
        geo_dict["TRACT"] = ("Census Tract", 40)
        geo_dict["BLKGRP"] = ("Block Group", 46)
        geo_dict["CONCIT"] = ("Consolidated City", 47)
        geo_dict["AIANHH"] = ("American Indian Area/Alaska Native Area/ Hawaiian Home Land (Census)", 52)
        geo_dict["AIANHHFP"] = ("American Indian Area/Alaska Native Area/ Hawaiian Home Land (FIPS)", 56)
        geo_dict["AIHHTLI"] = ("American Indian Trust Land/ Hawaiian Home Land Indicator", 61)
        geo_dict["AITSCE"] = ("American Indian Tribal Subdivision (Census)", 62)
        geo_dict["AITS"] = ("American Indian Tribal Subdivision (FIPS)", 65)
        geo_dict["ANRC"] = ("Alaska Native Regional Corporation (FIPS)", 70)
        geo_dict["CBSA"] = ("Metropolitan and Micropolitan Statistical Area", 75)
        geo_dict["CSA"] = ("Combined Statistical Area", 80)
        geo_dict["METDIV"] = ("Metropolitan Statistical Area-Metropolitan Division", 83)
        geo_dict["MACC"] = ("Metropolitan Area Central City", 88)
        geo_dict["MEMI"] = ("Metropolitan/Micropolitan Indicator Flag", 89)
        geo_dict["NECTA"] = ("New England City and Town Area", 90)
        geo_dict["CNECTA"] = ("New England City and Town Combined Statistical Area", 95)
        geo_dict["NECTADIV"] = ("New England City and Town Area Division", 98)
        geo_dict["UA"] = ("Urban Area", 103)
        geo_dict["BLANK"] = ("BLANK", 108)
        geo_dict["CDCURR"] = ("Current Congressional District", 113)
        geo_dict["SLDU"] = ("State Legislative District Upper", 115)
        geo_dict["SLDL"] = ("State Legislative District Lower", 118)
        geo_dict["BLANK"] = ("BLANK", 121)
        geo_dict["BLANK"] = ("BLANK", 127)
        geo_dict["BLANK"] = ("BLANK", 130)
        geo_dict["SUBMCD"] = ("Subminor Civil Division (FIPS)", 135)
        geo_dict["SDELM"] = ("State-School District (Elementary)", 140)
        geo_dict["SDSEC"] = ("State-School District (Secondary)", 145)
        geo_dict["SDUNI"] = ("State-School District (Unified)", 150)
        geo_dict["UR"] = ("Urban/Rural", 155)
        geo_dict["PCI"] = ("Principal City Indicator", 156)
        geo_dict["BLANK"] = ("BLANK", 157)
        geo_dict["BLANK"] = ("BLANK", 163)
        geo_dict["PUMA5"] = ("Public Use Microdata Area - 5\% File", 168)
        geo_dict["BLANK"] = ("BLANK", 173)
        geo_dict["GEOID"] = ("Geographic Identifier", 178)
        geo_dict["NAME"] = ("Area Name", 218)
        geo_dict["BTTR"] = ("Tribal Tract", 418)
        geo_dict["BTBG"] = ("Tribal Block Group", 424)
        geo_dict["BLANK"] = ("BLANK", 426)
        
        tableName = geoFile.strip().split("/")[-1].split(".")[0]
        dict_cols[tableName] = geo_dict
        self.createMetaTables(dict_cols)
        self.createGeoTables(dict_cols, singleFile)
        self.insertGeoDataFromFile(dict_cols, singleFile)
        
        
        
        
    
    def run(self):
        #This is always true because the queue.get command will raise an exception if empty 
        while True:
            singleFile = self.queue.get()

            if singleFile.seq_file != None:
                self.seqInsert(singleFile)
            elif singleFile.geo_file != None:
                self.geoInsert(singleFile)
            self.queue.task_done()
            
CONFIG_FORMAT = Template("""
#This is the basic configuration on the FTP server
[ftp]
#The FTP host
HOST=$ftp_host

#The 5 year base directory
BASE5=$acs5

#The summary file base directory name, typically summaryfile
SUMMARYFILE_BASE=$sumFolder_base

#The user tools base directory name
USERTOOLS=$userTools

#The geography base directory name
GEO=$geo

#This is the location on the server where all of the states
#have all of their tables ziped. See the file names in the ZIP* below
ALLTABLES=$allTables

[ftp files]
#This is the name of the template file. For 2010 data it is located:
# ftp://ftp.census.gov/acs2010_5yr/summaryfile/UserTools/2010_SummaryFileTemplates.zip 
TEMPLATE_FILE=$sumFileTemp

#The lower case geography name
GEO_LOWER=$geo_lower

#the upper case geography name
GEO_UPPER=$geo_upper

#If there are more than these two files to download
# make sure that the python dictionary and the configuration
# file generator is updated
 
#Zip file 1 to download
ZIP1=$zip1

#Zip file 2 to download
ZIP2=$zip2

[log file]
#The name of the log file
LOG=$logFile

#The verbosity level
# 1: ERROR and CRITICAL
# 2: WARNING
# 3: INFO
# 4: DEBUG
VERBOSE=$verbose

[others]
DATADIR=$data_dir
""")

def setupLog(logFile, verbose):
    verbose = int(verbose)
    if verbose == -1:
        verbose = 3
    fmt="%(asctime)s %(levelname)s: %(message)s"
    level = None
    if verbose == 1:
        level = logging.ERROR
    elif verbose == 2:
        level = logging.WARNING
    elif verbose == 3:
        level = logging.INFO
    elif verbose == 4:
        level = logging.DEBUG
    logging.basicConfig(filename=logFile, format=fmt, level=level)

def createConfig(year, conf_f, log_f, data_f, verbose):
    """
    While this could be hard coded and not be a dictionary, I like
    a dictionary for this in case someone else needs to update this. 
    Look at these variables to change and alter. Also this
    means that I get to use a string template, awesome!
    """
    state = "Massachusetts"
    config_dict = {"ftp_host": "ftp://ftp.census.gov", 
                   "acs5": "acs%s_5yr" % year,
                   "sumFolder_base" : "summaryfile",
                   "userTools" : "UserTools",
                   "sumFileTemp": "%s_SummaryFileTemplates.zip" % year,
                   "geo": "Geography",
                   "geo_lower": "ma.xls",
                   "geo_upper": "MA.xls",
                   "allTables": "%s-%s_ACSSF_By_State_All_Tables" % (year-4, year),
                   "zip1": "%s_Tracts_Block_Groups_Only.zip" % state,
                   "zip2": "%s_All_Geographies_Not_Tracts_Block_Groups.zip" % state,
                   "logFile": log_f,
                   "verbose": verbose,
                   "data_dir": data_f }
    config = open(conf_f, "w")
    config.write(CONFIG_FORMAT.safe_substitute(config_dict))
    config.close()

def parseConfig(configFile):
    parser = ConfigParser.ConfigParser()
    parser.readfp(open(configFile, "r"))
    config_dict = {"ftp_host": parser.get("ftp", "HOST"), 
                   "acs5": parser.get("ftp", "BASE5"),
                   "sumFolder_base" : parser.get("ftp", "SUMMARYFILE_BASE"),
                   "userTools" : parser.get("ftp", "USERTOOLS"),
                   "sumFileTemp": parser.get("ftp files", "TEMPLATE_FILE"),
                   "geo": parser.get("ftp", "GEO"),
                   "geo_lower": parser.get("ftp files", "GEO_LOWER"),
                   "geo_upper": parser.get("ftp files", "GEO_UPPER"),
                   "allTables": parser.get("ftp", "ALLTABLES"),
                   "zip1": parser.get("ftp files", "ZIP1"),
                   "zip2": parser.get("ftp files", "ZIP2"),
                   "logFile": parser.get("log file", "LOG"),
                   "verbose": parser.get("log file", "VERBOSE"),
                   "data_dir": parser.get("others", "DATADIR") }
    return config_dict
    
def unpackFiles(config_dict):
    queue = Queue.Queue()
    for i in range(5):
        t = ThreadUnzip(queue)
        t.setDaemon(True)
        t.start()
    queue.put("%s/%s" % (config_dict["data_dir"], config_dict["zip1"]))
    queue.put("%s/%s" % (config_dict["data_dir"], config_dict["zip2"]))
    queue.put("%s/%s" % (config_dict["data_dir"], config_dict["sumFileTemp"]))
    queue.join()

def putIntoDB(folder_dict):
    queue = Queue.Queue()
    #TODO: make 20 a -j flag default to 10. 
    for i in range(20):
        t = ThreadFiles(queue)
        t.setDaemon(True)
        t.start()
    #for x in folder_dict.keys():
    #    queue.put(folder_dict[x])
    queue.put(folder_dict[1])
    queue.put(folder_dict["geo0"])
    queue.join()
        
def parseArgs(argv):
    parser = argparse.ArgumentParser(description= """
This will put in ACS data correctly into a database. The first thing to do is to
create a config file, which can be automatically created with a 'Best Guess'. The
config file output might need to updated in this application to correctly output
new defaults if things like the census ftp changes. All of the options
are written to the configuration file but can be overridden when run. """)
    parser.add_argument("--createConfig", default=False, action='store_true', required=False, help="""
This will create the configuration file. If the --conf flag is not passed
the default file location and name will be used. --year needs to be
passed as a parameter if this option is used.""")
    parser.add_argument("-y", "--year", nargs=1, default=-1, type=int, required=False, help="""
This is the year to get data for as YYYY format. 
This is required for the createConfig parameter and is used as the base 
year for quite a lot of file names. For now, the only 5 year data will
be retrieved. This might be expanded later. There is no default for this
option.
The expansion will create a num_year variable and update the dictionary. 
""")
    parser.add_argument("-d", "--data", nargs=1, required=False, help="""
This is the path to the data directory. Please make sure
that you have read AND write access to whichever directory is chosen.
This will be the download directory for all of the data and
the unpack location. The default is /current working directory/data""")  
    parser.add_argument("-c", "--conf", nargs=1, required=False, help="""
This is the path to the configuration file used. This will default to
'/current working directory/acs.conf'. The current working directory is most
likely the directory which this application is run from. Please make sure
that you have read AND write access to whichever directory is chosen. This will 
also be the root directory used in the configuration file.""")
    parser.add_argument("-l", "--log", nargs=1, required=False, help="""
This is the path for the log file. Please make sure that you have read
AND write access to whichever directory is chosen. The default is
/current working directory/acs.log""")
    parser.add_argument("-v", "--verbose", default=-1, required=False, action="count", help="""
Defines the log level. The more v's the finer the logging. The default is 
to log at the info level.
-v:     ERROR and CRITICAL
-vv:    WARNING
-vvv:   INFO
-vvvv:  DEBUG""")
    cwd = os.getcwd()
    args = parser.parse_args()

    if args.createConfig == True and args.year != -1:
        #Create the log file
        conf_f = "%s/acs.conf" % cwd
        log_f = "%s/acs.log" % cwd
        data_f = "%s/data/" % cwd

        if args.conf != None:
            conf_f = args.conf[0]
        if args.log != None:
            log_f = args.log[0]
        if args.data != None:
            data_f = args.data[0]
        
        setupLog(log_f, args.verbose)
        logging.info("Starting the creation of the config file.")
        createConfig(args.year[0], conf_f, log_f, data_f, args.verbose)
        logging.info("Finished creation of the config file.")
    elif args.createConfig == True and args.year == -1:
        log_f = "%s/acs.log" % cwd
        if args.log != None:
            log_f = args.log
        setupLog(log_f, args.verbose)
        logging.critical("Tried to create a config file but the year was not passed. Now exiting.")
        print "Tried to create a config file but the year was not passed. Now exiting."
        #log a critical error and exit
        return
    elif args.createConfig == False:
        conf_f = "%s/acs.conf" % cwd
        if args.conf != None:
            conf_f = args.conf[0]
        try:
            config = open(conf_f, "r")
            config.close()
        except IOError as e:
            print "Config file not found at: %s. Please check that the file exists or pass a different config file" % conf_f
            sys.exit(0)
            
        config_dict = parseConfig(conf_f)

        if args.log != None:
            config_dict["logFile"] = args.log[0]
        if args.data != None:
            config_dict["data_dir"] = args.data[0]
        if args.verbose != -1:
            config_dict["verbose"] = args.verbose
        setupLog(config_dict["logFile"], config_dict["verbose"])
        
        
        #Note that all of this is multithreaded to speed up the process
        #Be very careful and test quite well. This all depends on a queue
        #This is great, but the queue will get very large, very fast.  
        #get the zip files from the ftp
        #unpack the zips
        logging.info("unpacking zip files")
        #unpackFiles(config_dict)
        logging.info("finished unpacking zip files")
        
        data_base = config_dict["data_dir"]
        template = "%s/%s" % (data_base, config_dict["sumFileTemp"].split(".")[0])
        folder1 = "%s/%s" % (data_base, config_dict["zip1"].split(".")[0])
        folder2 = "%s/%s" % (data_base, config_dict["zip2"].split(".")[0])
        folder_dict = Files.createTupples(config_dict, template, [folder1, folder2])
        putIntoDB(folder_dict)
        #get the zip files into a database
        #1) Pass the config file
        #2) List all of the file names
        #3) regex the file name to get e(YYYY)5ma%d.txt where the %d should be divided by 1000 (ma is geo_lower.split(".")[0])
        #4) m(YYYY)5ma%d.txt where the %d should be divided by 1000
        #5) g(YYYY)5ma.txt
        #6) the g file is a bitch, look at the pdf to make sure that it's done correctly. 
        return

def main(argv):
    parseArgs(sys.argv)
    logging.shutdown()
main(sys.argv)



