# American Community Survey import into PostGreSQL (python script)

This application takes [American Community survey (ACS)](http://www.census.gov/acs/) data and inserts it into a postgres database. 
Download ACS data (1, 3 or 5 year data) which can be found by looking at the [ACS FTP folders](http://www2.census.gov/).
Unzip the data files and run the python script to insert the raw text files into a database. 

This introduction will guide the user in how to setup
the environment and a basic series of steps that should be performed in a specific order. 

Assumptions: 	1. PostGreSQL database is installed with a user account that has permission to read, write and delete tables. 
				2. Python is installed with the ability to install required libraries
				3. Data zip files are downloaded from the Census website to a local (non-network drive) that the database can access.
				4. This has only been tested in Windows computers.
				
Section 2 will go over what actions are performed and how. 
The last section will have some brief SQL statements that will be useful for creating additional views from the data.

To see the help run: 

    $ python main.py -h

## Initial setup
 1. Download the data from Census website [ACS FTP folders](http://www2.census.gov/). This includes 2 or 3 zips files and an Excel (xls) file. 
    Save and unzip the data to a non-network drive. Example: C:\Python_ACS_code
 
	5 year average ACS Downloads:
	<state_name>_All_Geographies_Not_Tracts_Block_Groups.zip
	<state_name>_All_Geographies_Tracts_Block_Groups_Only.zip
	<year>_Summary_FileTemplates.zip
	<state_abbreviation>.xls
	
	Example for Massachusetts 2009-13, 5 year Average:
	http://www2.census.gov/acs2013_5yr/summaryfile/2009-2013_ACSSF_By_State_All_Tables/Massachusetts_All_Geographies_Not_Tracts_Block_Groups.zip
	http://www2.census.gov/acs2013_5yr/summaryfile/2009-2013_ACSSF_By_State_All_Tables/Massachusetts_All_Geographies_Tracts_Block_Groups_Only.zip
	http://www2.census.gov/acs2013_5yr/summaryfile/UserTools/2013_Summary_FileTemplates.zip
	http://www2.census.gov/acs2013_5yr/summaryfile/UserTools/Geography/ma.xls
	
	Example for United States 2013, 1 year Average:
	http://www2.census.gov/acs2013_1yr/summaryfile/2013_ACSSF_By_State_All_Tables/UnitedStates_All_Geographies.zip
	http://www2.census.gov/acs2013_1yr/summaryfile/UserTools/2013_Summary_FileTemplates.zip
   	http://www2.census.gov/acs2013_1yr/summaryfile/UserTools/Geography/Mini_Geo.xls - separate worksheets for each state
																					- save the specific geography in a new Excel spreadsheet. Example: us.xls or ma.xls

	Example of the Folder Structure:
	C:\Python_ACS_code
	C:\Python_ACS_code\data
	C:\Python_ACS_code\data\Massachusetts_All_Geographies_Not_Tracts_Block_Groups
	C:\Python_ACS_code\data\Massachusetts_All_Geographies_Tracts_Block_Groups_Only
	C:\Python_ACS_code\data\ma.xls
	C:\Python_ACS_code\data\2013_Summary_FileTemplates 

 
This application does not create a database or a database user within PostgreSQL. This must be specified in advance. 
The user permissions should be fairly restrictive or a temporary user
because the password has to be stored in the configuration file. The user will have to be able to
create tables, drop tables, create views, drop views, and add rows to a table. If the user is not given
drop table privileges, unexpected results will happen but most likely everything will fail due to primary
key constraint issues.  

When storing the data you will need a new database for each acs release (5 year average, 3 year average or 1 year) you are trying to load. 
A specific database name is not required, but the schema within the database is 
Database structure:

	Database: 	acs0913 (name for storing the current acs files) examples: acs0711, acs2013
	Schema: 	public


The requirements.txt file list the extensions that are needed to run the Python scripts. The versions may vary depending on your computer. 	 
 	argparse==1.2.1
 	configparser==3.2.0r3
 	ordereddict==1.1
 	psycopg2==2.4.5
 	unittest2==0.5.1
	wsgiref==0.1.2
 	xlrd==0.7.7


the acs.conf file is the configuration file that needs to be modified for the data. 
It is comprised of 4 sections: ftp files, log file, database and others.
An example of the file is below.

####################################################
[ftp file]

# name of the folder containing the Seq files
TEMPLATE_FILE=2013_Summary_FileTemplates

# ACS year(s): 1,3 or 5 year average
ACS_YEAR=1

#The lower case 2 letter geography name
GEO_LOWER=us

#folder extracted from the first Zip file
ZIP1=UnitedStates_All_Geographies

#folder extracted from the second Zip file. If processing 1 year acs without a second zip file enter: n/a
ZIP2=n/a

# ####################################################
[log file]
#The name of the log file
LOG=acs_log.log

#The verbosity level
# 1: ERROR and CRITICAL
# 2: WARNING
# 3: INFO
# 4: DEBUG
VERBOSE=4

# ####################################################
[database]
#The host to connect to
HOST=localhost

#The port to use
PORT=5432

#The database on host to use
DATABASE=acs0913

#The user name and password with write access to the database
USER=postgres
PASSWORD=##########

# ####################################################
[others]
DATADIR=C:/Python_ACS_Code/data

#This is the max number of threads to use for loading data. More threads causes thrashing
#A good starting number is to use n+1 where n is the number of cores in the computer running the application.
J=10

#using a single thread for sequentially loading the table shells
k=1

#This is the number of rows to batch in the txt before writing to the database.
BATCHROWS=1000
	
####################################################


# What the application does

This application will read in the unzipped ACS data from the data folder. 
The config file can be stored with the parameters altered for different ACS releases. The data structure cannot be altered.

# Database Structure
Once the application has been successfully run, the database will be populated from all of the e* and m* files (estimates and margin of errors are stored separately). 
The geography tables will be populated from the g* files. 

There are many tables in the e* and m* files. If one chooses to inspect the e* and m* files one will find that the column names are of the format (table name)_(number). There can be many tables in the same file, each with their own number.

This structure is put into the database as many tables structured as (table name)_e, (table name)_m and (table name)_meta. The meta-table contains human readable column names for that particular table. For example it will map column \_001 to "Total...". The same thing, more or less, happens with the geofile. The script will also automatically create a view called (table name) which will alternate the e* columns and the m* columns with names (column name), (column name)\_error respectively. 

once the tables are loaded into the database, views are created that combine the estimates and the margins of error.

# running the code

once the data, database, python libraries and configuration file are in place, 
you can run the python code for create_acs_tables.py either through python or by right clicking on the file and selecting edit with idle then run module.

the acs_log.log is generated when you run the code with any error messages and progress logs.