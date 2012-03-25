import psycopg2, re, os
from psycopg2 import ProgrammingError
import xlrd
import threading
import logging
from DBOps import DBOps

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
    def createTupples(cls, state_name_lc, seq_folder, folders=[]):
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
        e_re = re.compile(r"e(?P<year>\d+)%s%s(?P<id>\d+)\.txt" %
                          ("5", state_name_lc))
        m_re = re.compile(r"m(?P<year>\d+)%s%s(?P<id>\d+)\.txt" %
                          ("5", state_name_lc))
        g_re = re.compile(r"g(?P<year>\d+)%s%s\.txt" %
                          ("5",state_name_lc))
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
    """
    This class will take the output of Files.createTupples() and 
    insert data into the database.
    """
    def __init__(self, queue, db_host, db_port, db_database, db_user, db_pass, batchRows=200):
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
            self.myDBOpts.createTable( table, None, None, [("header", "varchar(10)"), ("description", "varchar(255)")] )
            
            data = []
            for k in sorted(dict_cols[key].keys()):
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
            f = open(e_file, "r")
            x = 0
            for line in f.readlines():
                if x == 10: #The frsit 10 rows should contain enough data to make this realistic. 
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
                headers.append((self.myDBOpts.colName(col), colType))
                
            self.myDBOpts.createTable( table, ok_head, None, headers )
            
            #create the m table
            table = "%s_m" % key
            headers = []
            for col in sorted(dict_cols[key].keys()): 
                colType = colTypes[dict_cols[key][col][1]]
                if colType == "null":
                    colType = "varchar(255)"
                headers.append((self.myDBOpts.colName(col), colType))
            self.myDBOpts.createTable( table, ok_head, None, headers )
    
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
    
    def createGeoTables(self, dict_cols, singleFile):
        """
        This will create the geo tables from the geo files.
        """
        dict_lookup = {}
        for tableName in dict_cols:
            headers, dict_lookup = self.lookupFromCols(dict_cols[tableName])
            
            #column types try to read in from file, if nothing, assume string of specified length but for now 255
            headerType = {}
            geo_f = open(singleFile.geo_file)
            for line in geo_f.readlines():
                line = self.parseGeoLine(dict_lookup, line, strip=False)
                headerType = self.myDBOpts.colTypeFromArray(line, headerType)
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
            self.myDBOpts.createTable(tableName, pk_head, None, other_head)
    
    def batchGeoInsert(self, table, col_names, data):
        """
        A separate geo insert function is needed since
        this data is much less complicated than the e* and m* data.
        """ 
        logging.debug("Pushing data")
        self.myDBOpts.insert(table, col_names, data)
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
        """
        This will push all of the data in the data array
        into tableName_tableSuffix. This is really tricky
        because it will insert N rows into K tables where each
        table is defined in data.keys() and dict_cols.keys()
        """
        logging.debug("Pushing data")
        #for every table defined in data, generate table specific column names
        for tableName in data.keys():
            col_names = []
            col_names.append("LOGRECNO") #TODO: somehow make this not a static value
            #column names for each table are defined as the second level of keys
            #from dict_cols. Make sure it is sorted to match the format the the data.
            for col_name in sorted(dict_cols[tableName].keys()):
                col_names.append(self.myDBOpts.colName(col_name))
            logging.info("Inserting data to table: %s_%s" % (tableName, table_suffix))
            self.myDBOpts.insert("%s_%s" % (tableName, table_suffix), col_names, data[tableName] )
        logging.debug("Finishing pushing data")
            
    def insertDataFromFile(self, file_handle, table_suffix, dict_cols, pk_index):
        """
        file_handle: An open read-only file handle pointing to a data file.
        table_suffix: This is added to the table name in the form: tableName_suffix.
                      Typically this value should only be m or e, depending on the file
                      being read.
        pk_index: Column index for the primary key, 0 indexed
        """
        data = {}
        counter = 1
        #It is important to point out that the following occurs for each line
        #in the CSV. The idea is that each line in the CSV contains data for K
        #tables and table must have it's own data stored correctly.
        for e_line in file_handle.readlines():
            if (counter % self.batchRows) == 0:
                self.batchInsert(data, table_suffix, dict_cols)
                data = {} #Clear the data since it's now in the database
                counter = 1 #reset the counter to make sure it doesn't get too big
            e_line = e_line.split(",") #generate array from each line in the CSV
            for table_name in dict_cols.keys():
                data_line = [e_line[pk_index]] #the 0 separator is assumed to be the pk index
                if table_name == "all": continue #the all keyword is special and has in it only the shared columns
                if table_name not in data.keys(): #if the table name doesn't exist in the data dictionary, create an array
                    data[table_name] = []
                for col in sorted(dict_cols[table_name].keys()): #for each column in the table
                    value, col_idx = dict_cols[table_name][col] #get the meta data value and the column index
                    #append each cell in the line to the correct position in the data_line
                    #note that the data_line is a single row in a specific table.
                    #There are many tables in a single CSV row.
                    data_line.append(e_line[col_idx].strip()) 
                    
                #logging.debug("table name %s: data_line length: %s, number of columns in table: %s" %
                #              ( table_name, len(data_line), len(dict_cols[table_name].keys())+1 ))
                #Append the data to correct specific table. This is done for each
                #table defined by the column headers. We are still in the table_name for loop.
                data[table_name].append(data_line)
            counter = counter + 1
        #Finish off inserting the extra data not written durring the batching.
        self.batchInsert(data, table_suffix, dict_cols)
        
    
    def insertTableData(self, dict_cols, singleFile, col_seperators):
        """
        singleFile: an instance of Files
        col_seperators: A list of column indexes which separates the different tables
                        contained in a single file. For more information see the README.
        """         
        for x in range(len(singleFile.e_files)):
            e_file = open(singleFile.e_files[x])
            m_file = open(singleFile.m_files[x])
            self.insertDataFromFile(e_file, "e", dict_cols, col_seperators[0])
            self.insertDataFromFile(m_file, "m", dict_cols, col_seperators[0])
            e_file.close()
            m_file.close()
    
    #TODO: Figure out a good way to put this into DBOps. Creating views is very
    #easy but highly customizable. Think about a good way to go about doing that. 
    def createViews(self, dict_cols, singleFile):
        """
        Creates the default views for all of the e and m tables.
        """
        for table_name in dict_cols.keys():
            if table_name == "all": continue
            logging.info("Dropping view %s" % table_name)
            cmd_str = "DROP VIEW %s;" % table_name
            self.myDBOpts.execute(cmd_str)
            logging.info("creating view %s" % table_name)
            cmd_str = "CREATE OR REPLACE VIEW %s AS SELECT e.LOGRECNO as LOGRECNO, \n" % table_name
            for col in sorted(dict_cols[table_name].keys()):
                col_name = self.myDBOpts.colName(col)
                cmd_str = "%se.%s as %s, m.%s as %s_error, \n" % (cmd_str, col_name, col_name, col_name, col_name)
            cmd_str = cmd_str.strip(", \n")
            cmd_str = "%s \nfrom %s_e e \njoin %s_m m on e.LOGRECNO = m.LOGRECNO;" % (cmd_str, table_name, table_name)
            self.myDBOpts.execute(cmd_str)
    
    def seqInsert(self, singleFile):
        """
        This is the main function when dealing with an e* or m* file.
        This will generate the dict_cols, create the meta tables,
        insert the table data and create the default views when the
        Files instance is passed.
        """
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
        self.createViews(dict_cols, singleFile)
    
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