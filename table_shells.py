import psycopg2, re, os
from psycopg2 import ProgrammingError
import xlrd
import threading
import logging
from db_cmds import DBOps

class Shells():
    #seq_file = None denotes a geo file
    #add an assertion here 
    def __init__(self, seq_file=None, geo_file=None):
        assert seq_file != None or geo_file != None
        self.seq_file = seq_file
        self.e_files = []
        self.m_files = []
        self.geo_file = geo_file
    
    @classmethod
    def createTupples(cls, acs_year, state_name_lc, seq_folder, folders=[]):
        """
        * state_name_lc: This is the lower case state name. This is used
                         to construct the e* and m* file names.
        * seq_folder: This is the full path location to the sequence files.  
                      The folder should contain the SeqX.xls files.
        * folders: This is a list of folders which contain the 
                   e*, m* and a single g* file. The list returned will be a list of
                   Files objects. The 'files' object will be a list of tupples, (e*, m*),
                   folder locations. 
       This will generate the input dictionary for ThreadedFiles.run()
        """
        seq_re = re.compile(r"Seq(?P<id>\d+)\.xls")
        e_re = re.compile(r"e(?P<year>\d+)%s%s(?P<id>\d+)\.txt" % (state_name_lc, acs_year))
        m_re = re.compile(r"m(?P<year>\d+)%s%s(?P<id>\d+)\.txt" % (state_name_lc, acs_year))
        g_re = re.compile(r"g(?P<year>\d+)%s%s\.txt" % (state_name_lc, acs_year))
        
        #logging.info("e(?P<year>\d+)%s%s(?P<id>\d+)\.txt" % (state_name_lc, acs_year))
        seqs = os.listdir(seq_folder)
        folder_dict = {}
        geo = []
        
        for seq in seqs:
            s = seq_re.match(seq)
            if s != None:
                id = int(s.groups("id")[0])
                folder_dict[id] = Shells("%s/%s" % (seq_folder, seq))
        
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
                        folder_dict[id] = Shells(seq_file=None, geo_file="%s/%s" % (f, file))
        return folder_dict
                        
class CreateTableShells(threading.Thread):
    """
    This class will take the output of Files.createTupples() and 
    insert data into the database.
    """
    def __init__(self, queue, db_host, db_port, db_database, db_user, db_pass, batchRows):
        """
        * queue: The shared threading queue
        * db_host: The database host
        * db_database: The database to connect to
        * db_user: The database username
        * db_pass: The database password
        * batchRows: The number of rows to batch for each insert
        """
        threading.Thread.__init__(self)
        self.queue = queue
        self.myDBOpts = DBOps(db_host, db_port, db_database, db_user, db_pass)
        self.batchRows = batchRows
        self.col_re = re.compile(r"(?P<table_name>\w+)_(?P<col_name>\d+)")
            
    def createMetaTables(self, dict_cols):
        """
        This creates the meta tables where the column name and
        description are the rows of the table for every table
        defined in dict_cols
        """        
        for key in dict_cols.keys():
            if (key == "all"): continue
            table = "%s_meta" % key
            DoesTblExist = None 
            #cmd_str = "SELECT EXISTS (SELECT * from %s);" % table_name ## check if table exists
            cmd_str = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE  n.nspname = 'public' AND c.relname = '%s');" % table.lower()  
            DoesTblExist = self.myDBOpts.execute(cmd_str) #returns true if it does so data must be appended or false to create a new table

            #logging.info("DoesTblExist %s" % DoesTblExist)
            if DoesTblExist == [(False,)]: 
                #logging.info("New Meta %s" % table)
                self.myDBOpts.createTable( table, None, None, [("header", "varchar(10)"), ("description", "text")] )
                
                data = []
                for k in sorted(dict_cols[key].keys()):
                    data.append([k, dict_cols[key][k][0]])
                self.myDBOpts.insert(table, ["header", "description"], data)
                
            else: # if DoesTblExist = true - need to append
                #logging.info("Append meta %s" %  dict_cols[key][0])
                data = []
                for k in sorted(dict_cols[key].keys()):
                    data.append([k, dict_cols[key][k][0]])
                    #logging.info("Append meta %s" % k)
                    DoesColExist = None
                    cmd_str = "SELECT EXISTS (SELECT %s FROM %s);" % (k, table.lower()) 
                    DoesColExist = self.myDBOpts.execute(cmd_str)  
                    if DoesColExist == [(False,)]: 
                       data.append([k, dict_cols[key][k][0]])
                self.myDBOpts.insert(table, ["header", "description"], data)
    
    def columnTypes(self, dict_cols, files):
        """
        This will go through each of the e files and try to decern which db type
        the column is suppose to be and return the headerType dictionary keyed
        to column index. Only the first line is used. This seems to work well. 
        This will not work for geo files.
        """
        headerType = {}
        for e_file in files.e_files:
            #logging.info("e_file: %s" % e_file)
            f = open(e_file, "r")
            x = 0
            for line in f.readlines():
                if x == 10: #The first 10 rows should contain enough data to make this realistic. 
                    f.close()
                    return headerType 
                line = line.split(",")    
                headerType = self.myDBOpts.colTypeFromArray(line, headerType)
                x = x + 1
                
            f.close()

        return headerType
                
    def createTables(self, dict_cols, singleFile):
        """
        This creates the tables from the information stored in dict_cols.
        This does not input any data into the tables.
        """
        
        #pk_logRecNo tells if the table in pk_logRecNo
        ok_head = None
        if "LOGRECNO" in dict_cols["all"].keys():
            ok_head = ("LOGRECNO", "integer")
        
        colTypes = self.columnTypes(dict_cols, singleFile) # returns a dict
        
        for key in sorted(dict_cols.keys()):
            if (key == "all"): continue

            ## run sql to check if table exists

            #create the e estimates table
            table = "%s_e" % key
            headers = []
            DoesTblExist = None 
            #cmd_str = "SELECT EXISTS (SELECT * from %s);" % table_name ## check if table exists
            cmd_str = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE  n.nspname = 'public' AND c.relname = '%s');" % table.lower() 
            DoesTblExist = self.myDBOpts.execute(cmd_str) #returns true if it does so data must be appended or false to create a new table
            
            for col in sorted(dict_cols[key].keys()):
                colType = colTypes[dict_cols[key][col][1]] # associates column type with column in a table
                if colType == "null":
                    colType = "varchar(255)"
                headers.append((self.myDBOpts.colName(col), colType))

            if DoesTblExist == [(False,)]:     
                self.myDBOpts.createTable(table, ok_head, None, headers ) # passes table name, primary key, None, headers array
            else:
                self.myDBOpts.appendTable(table,  None, headers )

            #create the m table
            table = "%s_m" % key
            headers = []
            for col in sorted(dict_cols[key].keys()): 
                colType = colTypes[dict_cols[key][col][1]]
                if colType == "null":
                    colType = "varchar(255)"
                headers.append((self.myDBOpts.colName(col), colType))

            if DoesTblExist == [(False,)]:     
                self.myDBOpts.createTable(table, ok_head, None, headers ) # passes table name, primary key, None, headers array
            else:
                self.myDBOpts.appendTable(table,  None, headers )

    def lookupFromCols(self, dict_cols_item):
        """ 
        This will generate the dict_lookup and headers
        for each dict_cols item.
        """
        
        headers = []
        dict_lookup = {}
        for header in dict_cols_item.keys():
            dict_lookup[dict_cols_item[header][1]] = header
        
        for key in sorted(dict_lookup.keys()):
            headers.append(dict_lookup[key])
        return (headers, dict_lookup)
    
    def seqInsertShells(self, singleFile):
        """
        This is the main function when dealing with an e* or m* file.
        This will generate the dict_cols, create the meta tables,
        insert the table data and create the default views when the
        Files instance is passed.
        """
        book = xlrd.open_workbook(singleFile.seq_file)
        #There are always two sheats, e and m. They are exactly the same
        sheet = book.sheets()[0]
        dict_cols = {"all" : {}}
        sorted_dict_cols = {"all" : {}}

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
        logging.info(" FINISHED sequence shell:  %s" % singleFile.seq_file)

    
    def run(self):
        #This is always true because the queue.get command will raise an exception if empty 
        while True:
            singleFile = self.queue.get()

            if singleFile.seq_file != None:
                self.seqInsertShells(singleFile)
            self.queue.task_done()
