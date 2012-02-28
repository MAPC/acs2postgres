"""
ftp://ftp.census.gov/
acs2010_5yr/ - where the filename is acsYYYY_5yr
summaryfile/(YYYY-4)-YYYY_ACSSF_By_State_All_Tables/
ls Mass*

grab these two files, unzip them
 
 next
 ftp://ftp.census.gov/
 acsYYYY_5yr/summaryfile/UserTools/
 YYYY_SummaryFileTemplates.zip
 
 read this: http://www2.census.gov/acs2010_5yr/summaryfile/ACS_2006-2010_SF_Tech_Doc.pdf
 
 according to this documentation p13 (11) figure out fixed width columns so header for the g* file 
 1 geo table
 
 
 e* and m* csv files are related
 header for e* and m* located in \2010_SummaryFileTemplates where Seq#.xls is linked to e*ma#000.csv   
 
 ftp://ftp.census.gov/
 acsYYYY_5yr/summaryfile/UserTools/Geography/
 get either ma.xls or MA.xls
 
 ftp://ftp.census.gov/acs2010_5yr/summaryfile/2006-2010_ACSSF_By_State_All_Tables/Massachusetts_Tracts_Block_Groups_Only.zip
 ftp://ftp.census.gov/acs2010_5yr/summaryfile/2006-2010_ACSSF_By_State_All_Tables/Massachusetts_All_Geographies_Not_Tracts_Block_Groups.zip
 
 
 When creating tables use the B##### C##### not including _AAA. The AAA corresponds to a type of column. 
 The type of column is going to be put into a meta field, so dictionary. The estimator files are going to populate the _AAA fields.
 This will be joined with the m* file to population the _AAA_means file. The geo table needs to be joined on the geo db. 
 
 Order to do this in so far:
 Figure out a way to download from the FTP the various files. 
 Unzip the various files
 Figure out a way to dict up all of the tables and create them all in one go, note that this might have
     to be done very carefully. The dictionaries could become huge. Be careful of this. 
     
Each table should have the LOGRECNO as the primary key. There are many tables within a spreadsheet with many columns.
    Again, be very careful about how to organize this.  

All data can be found using the above links. This might have to change for previous years. Think about how to expand this to 
    make sure the various years are taken into account. 

TODO: Create database upon creation of config file with the year
TODO: Pass in database options to the config file and take them on the cmd
CREATE DATABASE y2010
  WITH ENCODING='LATIN1'
       CONNECTION LIMIT=-1; 
"""


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
    def __init__(self, seq_file):
        self.seq_file = seq_file
        self.e_files = []
        self.m_files = []
    
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
                    geo.append(file)
        return (folder_dict, geo)
                        
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
            conn = psycopg2.connect(host="localhost", dbname="y2010", user="ben", password="password")
            cur = conn.cursor()
            cur.execute(cmd)
            conn.commit()
        except:
            logging.error("A database operation failed. The command is: %s" % cmd)
        try:
            ret = cur.fetchall()
            return ret
        except ProgrammingError:
            return None
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
        
        self.execute("DROP TABLE %s CASCADE;" % tableName)
        
        cmd_str = "CREATE TABLE %s (" % tableName
        if pk_head != None:
            cmd_str = "%s %s %s, " % (cmd_str, pk_head[0], pk_head[1])
        else:
            cmd_str = "%s id serial, " % cmd_str
        
        for h, t in unique_head:
            cmd_str = "%s _%s %s, " % (cmd_str, h, t)
        
        for h, t in other_head:
            cmd_str = "%s _%s %s, " % (cmd_str, h, t)
        
        if len(unique_head) != 0:
            cmd_str = "%s UNIQUE( " % cmd_str
            for h, t in unique_head:
                cmd_str = "%s %s, " % (cmd_str, h)
            cmd_str = cmd_str.rstrip(",")
            cmd_str = "%s ) " % cmd_str
        cmd_str = cmd_str.rstrip(" ,") + (");")
        self.execute(cmd_str)
    
    def run(self):
        #This is always true because the queue.get command will raise an exception if empty 
        while True:
            files = self.queue.get()
            book = xlrd.open_workbook(files.seq_file)
            #There are always two sheats, E and M. They are exactly the same
            #print len(book.sheets())
            sheet = book.sheets()[0]
            dict_cols = {"all" : {}}
            csv_headers = []
            for col in range(sheet.ncols):
                csv_headers.append(sheet.cell(0, col).value)
                m = self.col_re.match(sheet.cell(0, col).value)
                if m == None:
                    dict_cols["all"][sheet.cell(0, col).value] = sheet.cell(1, col).value
                else:
                    #Use the fact that if the table_name is not in the dictionary yet
                    #maping csv and header information is non trivial. Multiple tables with a main
                    # make this a pain. Try to only work with say, 2000k inserts at a time. 
                    if m.group("table_name") not in dict_cols.keys():
                        dict_cols[m.group("table_name")] = {}
                    dict_cols[m.group("table_name")][m.group("col_name")] = sheet.cell(1, col).value
            pk = None
            if "LOGRECNO" in dict_cols["all"].keys():
                pk = ("LOGRECNO", "integer")

            for key in dict_cols.keys():
                if (key == "all"): continue
                col_names = []
                for k in sorted(dict_cols[key].keys()):
                    col_names.append((k, "varchar(255)"))
                self.createTable("%s_meta" % key, pk, None, col_names)
            
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

def putIntoDB(folder_dict, geo):
    queue = Queue.Queue()
    for i in range(5):
        t = ThreadFiles(queue)
        t.setDaemon(True)
        t.start()
    queue.put(folder_dict[1])
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
        folder_dict, geo = Files.createTupples(config_dict, template, [folder1, folder2])
        putIntoDB(folder_dict, geo)
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
main(sys.argv)



