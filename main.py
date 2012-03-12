# data retrieved from ftp://ftp.census.gov/acs2010_5yr/summaryfile/
# The geo pdf is the ftp://ftp.census.gov/acs2010_5yr/summaryfile/ACS_2006-2010_SF_Tech_Doc.pdf
import sys, argparse
import ConfigParser, os, logging
import Queue

from ThreadedZip import zipFile, ThreadUnzip
from ThreadedFiles import Files, ThreadFiles
from string import Template

            
CONFIG_FORMAT = Template("""
#This is the basic configuration on the FTP server
[ftp]
#The FTP host (not implemented)
HOST=$ftp_host

#The 5 year base directory (not implemented)
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

[database]
#The host to connect to
HOST=$host

#The port to use
PORT=$port

#The database on host to use
DATABASE=$database

#The user name and password with write access to the database
USER=$user
PASSWORD=$password

[others]
DATADIR=$data_dir
#This is the max number of threads to use. More threads causes thrashing
#A good starting number is to use n+1 where n is the number of cores in 
#the computer running the application.
J=$j
#This is the number of rows to batch before writing to the database.
#The more rows batched, the faster the inserts but the more ram required.
#If VRAM is starting to get hit, lower this number as that will slow down
#The inserts. Remember that this will batch rows in the CSV file which 
#could contain up to 15 tables which could be quite a lot of data. 
BATCHROWS=$batch_rows
""")

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

def createConfig(year, conf_f, log_f, data_f, verbose, j, batchRows, host, port,
                 database, user, password):
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
                   "data_dir": data_f,
                   "j": j,
                   "batch_rows": batchRows,
                   "host": host,
                   "port": port,
                   "database": database,
                   "user": user,
                   "password": password }
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
                   "data_dir": parser.get("others", "DATADIR"),
                   "j": int(parser.get("others", "J")),
                   "batch_rows": int(parser.get("others", "BATCHROWS")),
                   "host": parser.get("database", "HOST"),
                   "port": parser.get("database", "PORT"),
                   "database": parser.get("database", "DATABASE"),
                   "user": parser.get("database", "USER"),
                   "password": parser.get("database", "PASSWORD")}
    return config_dict
    
def unpackFiles(config_dict, j):
    queue = Queue.Queue()
    for i in range(j):
        t = ThreadUnzip(queue)
        t.setDaemon(True)
        t.start()
    zip1 = zipFile("%s/%s" % (config_dict["data_dir"], config_dict["zip1"]))
    zip2 = zipFile("%s/%s" % (config_dict["data_dir"], config_dict["zip2"]))
    templateFile = zipFile("%s/%s" % (config_dict["data_dir"], config_dict["sumFileTemp"]))
    
    queue.put(zip1)
    queue.put(zip2)
    queue.put(templateFile)
    queue.join()

def putIntoDB(folder_dict, j, host, port, database, user, password, batchRows):
    queue = Queue.Queue()
    for i in range(j):
        t = ThreadFiles(queue, host, port, database, user, password, batchRows=batchRows)
        t.setDaemon(True)
        t.start()
    
    for x in folder_dict.keys():
        queue.put(folder_dict[x])
    #queue.put(folder_dict[1])
    #queue.put(folder_dict["geo0"])
    queue.join()
        
def parseArgs(argv):
    parser = argparse.ArgumentParser(description= """
This will put in ACS data correctly into a database. The first thing to do is to
create a config file, which can be automatically created with a 'Best Guess'. The
config file output might need to updated in this application to correctly output
new defaults if things like the census ftp changes. All of the options
are written to the configuration file but can be overridden when run. """)
    ###########################
    #THE CREATE CONFIG OPTIONS#
    ###########################
    parser.add_argument("--createConfig", default=False, action='store_true', required=False, help="""
This will create the configuration file. If the --conf flag is not passed
the default file location and name will be used. --year needs to be
passed as a parameter if this option is used.""")
    parser.add_argument("-y", "--year", nargs=1, default=-1, type=int, required=False, help="""
This is the year to get data for as YYYY format. 
This is required for the createConfig parameter and is used as the base 
year for quite a lot of file names. For now, the only 5 year data will
be retrieved. This might be expanded later. There is no default for this
option.""")
    parser.add_argument("--host", nargs=1, default="", required=False, help="""
This is the database host to connect to. --host must be passed when the
--createConfig flag is passed.""")
    parser.add_argument("--port", nargs=1, default=-1, type=int, required=False, help="""
This is the database port to connect to. The default is 5432.""")
    parser.add_argument("-u", "--user", nargs=1, default="", required=False, help="""
This is the user to login with. -u or --user must be passed when the
--createConfig flag is passed. There is no default.""")
    parser.add_argument("--password", nargs=1, default="", required=False, help="""
This is the password to login with. If the password is not passed when
--createConfig is passed the password will be empty. There is no default.""")
    parser.add_argument("-b", "--database", nargs=1, default="", required=False, help="""
This is the database to connect to on the host. -b or --database must be passed when the
--createConfig flag is passed. There is no default.""")
    ###########################
    #OTHER OPTIONS            #
    ###########################
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
    parser.add_argument("-j", "--numThreads", default=-1, type=int, required=False, help="""
This defines the max number of threads to use while running this program. 
The more threads, the more speadup but the more possible thrashing. A good
Typical value is n+1 where n is the number of cores in the computer running
the application. More can be used for tuning the application. The default
value is 2.""")
    parser.add_argument("--batchRows", default=-1, type=int, required=False, help="""
This defines the number of rows to batch before writing to the database. The
more rows the faster the application can run but the more ram required. Tune
this parameter to find the fastest speed for a particular computer. The default
value is 200""")
    cwd = os.getcwd()
    args = parser.parse_args()
    if args.createConfig == True:
        #Create the log file
        conf_f = "%s/acs.conf" % cwd
        log_f = "%s/acs.log" % cwd
        data_f = "%s/data/" % cwd
        port_d = 5432
        password = ""
        verbose = 3

        if args.conf != None:
            conf_f = args.conf[0]
        if args.log != None:
            log_f = args.log[0]
        if args.data != None:
            data_f = args.data[0]
        if args.port != -1:
            port_d = int(args.port[0])
        if args.password != "":
            password = args.password[0]
        if args.verbose != -1:
            verbose = args.verbose
        
        #If createConfig is passed the the following should be passed as well:
        #year, host, port (opional), user, password (optional), database
        if args.year == -1:
            setupLog(log_f, verbose)
            logging.critical("Tried to create a config file but the year was not passed. Now exiting.")
            print "Tried to create a config file but the year was not passed. Now exiting."
            return
        
        if args.host == "":
            setupLog(log_f, verbose)
            logging.critical("Tried to create a config file but the database host was not passed. Now exiting.")
            print "Tried to create a config file but the database host was not passed. Now exiting."
            return
        
        if args.user == "":
            setupLog(log_f, verbose)
            logging.critical("Tried to create a config file but the database user was not passed. Now exiting.")
            print "Tried to create a config file but the database user was not passed. Now exiting."
            return
        
        if args.database == "":
            setupLog(log_f, args.verbose)
            logging.critical("Tried to create a config file but the database to use was not passed. Now exiting.")
            print "Tried to create a config file but the database to use was not passed. Now exiting."
            return
        
        numThreads = 2
        if args.numThreads != -1:
            numThreads = int(args.numThreads)
        
        batchRows = 200
        if args.batchRows != -1:
            batchRows = int(args.batchRows)
        
        setupLog(log_f, args.verbose)
        logging.info("Starting the creation of the config file.")
        createConfig(args.year[0], conf_f, log_f, data_f, verbose,
                     numThreads, batchRows, args.host[0], port_d, args.database[0],
                     args.user[0], password)
        logging.info("Finished creation of the config file.")
    elif args.createConfig == True and args.year == -1:
        log_f = "%s/acs.log" % cwd
        if args.log != None:
            log_f = args.log
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
        if args.numThreads != -1:
            config_dict["j"] = int(args.numThreads)
        if args.batchRows != -1:
            config_dict["batch_rows"] = int(args.batchRows)
        #database overrides
        if args.host != "":
            config_dict["host"] = args.host[0]
        if args.port != -1:
            config_dict["port"] = int(args.port[0])
        if args.database != "":
            config_dict["database"] = args.database[0]
        if args.user != "":
            config_dict["user"] = args.user[0]
        if args.password != "":
            config_dict["password"] = args.password[0]
        
        
        setupLog(config_dict["logFile"], config_dict["verbose"])
        
        logging.debug("The config_dict is: %s" % config_dict)
        print config_dict
        return
        logging.info("unpacking zip files")
        unpackFiles(config_dict, config_dict["j"])
        logging.info("finished unpacking zip files")
        
        data_base = config_dict["data_dir"]
        template = "%s/%s" % (data_base, config_dict["sumFileTemp"].split(".")[0])
        folder1 = "%s/%s" % (data_base, config_dict["zip1"].split(".")[0])
        folder2 = "%s/%s" % (data_base, config_dict["zip2"].split(".")[0])
        folder_dict = Files.createTupples(config_dict["geo_lower"].split(".")[0], template, [folder1, folder2])
        putIntoDB(folder_dict, config_dict["j"], 
                  config_dict["host"], config_dict["port"],
                  config_dict["database"], config_dict["user"], 
                  config_dict["password"], config_dict["batch_rows"])
        return

def main(argv):
    parseArgs(argv)
    logging.shutdown()
main(sys.argv)



