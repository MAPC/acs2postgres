# data retrieved from ftp://ftp.census.gov/acs2013_5yr/summaryfile/
# The geo pdf is the ftp://ftp.census.gov/acs2013_5yr/summaryfile/ACS_2009-2013_SF_Tech_Doc.pdf
import sys, argparse
import ConfigParser, os, logging
import Queue

from table_shells import Shells, CreateTableShells
from data_tables import Files, ThreadFiles
from string import Template

            
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

def parseConfig(configFile):
    parser = ConfigParser.ConfigParser()
    parser.readfp(open(configFile, "r"))
    config_dict = {"sumFileTemp": parser.get("ftp files", "TEMPLATE_FILE"),
                   "geo_lower": parser.get("ftp files", "GEO_LOWER"),
                   "zip1": parser.get("ftp files", "ZIP1"),
                   "zip2": parser.get("ftp files", "ZIP2"),
                   "acs_year": parser.get("ftp files", "ACS_YEAR"),
                   "logFile": parser.get("log file", "LOG"),
                   "verbose": parser.get("log file", "VERBOSE"),
                   "data_dir": parser.get("others", "DATADIR"),
                   "j": int(parser.get("others", "J")),
		   "k": int(parser.get("others", "K")),
                   "batch_rows": int(parser.get("others", "BATCHROWS")),
                   "host": parser.get("database", "HOST"),
                   "port": parser.get("database", "PORT"),
                   "database": parser.get("database", "DATABASE"),
                   "user": parser.get("database", "USER"),
                   "password": parser.get("database", "PASSWORD")}
    return config_dict

def createTables(folder_dict, k, host, port, database, user, password, batchRows, isDebug):
    queue = Queue.Queue()
    for i in range(k):
        t = CreateTableShells(queue, host, port, database, user, password, batchRows) #extends the ThreadFiles library and makes threading class available
        t.setDaemon(False) # doesn't allow multi-threading to complete for this instantiation
        t.start() #triggers run in ThreadFiles
        
    if isDebug == False:
        for x in sorted(folder_dict.keys()):
            
            queue.put(folder_dict[x])
        else:
            queue.put(folder_dict[1]) #use the first key
    queue.join() # blocks all items in Queue until items in queue have been processed
    
def putIntoDB(folder_dict, j, host, port, database, user, password, batchRows, isDebug):
    queue = Queue.Queue()
    for i in range(j):
        t = ThreadFiles(queue, host, port, database, user, password, batchRows) #extends the ThreadFiles library and makes threading class available
        t.setDaemon(False) #allows multi-threading to complete for this instantiation
        t.start() #triggers run in ThreadFiles
        
    if isDebug == False:
        for x in sorted(folder_dict.keys()):
            
            queue.put(folder_dict[x])
        else:
            queue.put(folder_dict[1]) #use the first key
    queue.join() # blocks all items in Queue until items in queue have been processed
    
def parseArgs():
    cwd = os.getcwd() # The current working directory should have the config and python files. It is where the log file will be written.
    conf_f = "%s/acs.conf" % cwd 
    config = open(conf_f, "r")
    config.close()

    config_dict = parseConfig(conf_f)# creates the dictionary with the configuration settings
           
    setupLog(config_dict["logFile"], config_dict["verbose"]) # Creates the log file
    
    # assigning variables from the configuration   
    data_base = config_dict["data_dir"]
    template = "%s/%s" % (data_base, config_dict["sumFileTemp"])  # the Summary_FileTemplates folder     
    folder1 = "%s/%s" % (data_base, config_dict["zip1"])# <geo>_All_Geographies_Not_Tracts_Block_Groups folder
    acsyear = config_dict["acs_year"]
    #logging.info("acsyear %s" % acsyear)
    
    if config_dict["zip2"] == "n/a":
        #logging.info("Running 1yr data")
        folder_dict = Shells.createTupples(config_dict["geo_lower"], acsyear, template, [folder1]) # function in the Files class (ThreadedFiles.py)
    else:
        folder2 = "%s/%s" % (data_base, config_dict["zip2"])          # <geo>_Tracts_Block_Groups_Only folder
        folder_dict = Shells.createTupples(config_dict["geo_lower"], acsyear, template, [folder1, folder2]) # function in the Files class (ThreadedFiles.py)
        
    createTables(folder_dict, config_dict["k"], 
        config_dict["host"], config_dict["port"],
        config_dict["database"], config_dict["user"],
        config_dict["password"], config_dict["batch_rows"], 
        isDebug=False)
    logging.info("Finished Table Shells")
    
    putIntoDB(folder_dict, config_dict["j"], 
        config_dict["host"], config_dict["port"],
        config_dict["database"], config_dict["user"],
        config_dict["password"], config_dict["batch_rows"], 
        isDebug=False)
    logging.info("Finished!")
    return

def main(argv):
    parseArgs()
    logging.shutdown()
	
main(sys.argv)



