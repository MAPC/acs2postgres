#Introduction

This application takes ACS data and inserts it into a database. This introduction will guide the user in how to setup
the environment and a basic series of steps that should be performed in a specific order. Section 2 will go over what actions are
performed and how. The last section will have some brief SQL statements that will be useful for creating additional
views from the data.
   
To see the help run: python main.py -h
   
##Initial setup
The initial requires a bit of work. This application does not create a database or a database user. This 
must be specified in advance. The user permissions should be fairly restrictive or a temporary user
because the password has to be stored in the configuration file. The user will have to be able to
create tables, drop tables, create views, drop views, and add rows to a table. If the user is not given
drop table privileges, unexpected results will happen but most likely everything will fail due to primary
key constraint issues. 
  
The zip files should be downloaded and placed in the "data" folder. The data folder is assumed to be
"/current working dir/data". This can be changed with the -d, --data option when running the application. 
The zip files are found as zip1 and zip2 in the configuration files. Make sure that the names are correct
as these names are a best guess (though correct for 2010 5 year data). There should be only three zip files
in the data directory of the form YYYY_SummaryFileTemplates.zip, STATENAME_All_Geo..., 
and STATENAME_Tract_Block.... these zip files will be unziped in the data directory into a folder named
the zip file. 
   
As a quick racap: A database and database need to be created and the zip files downloaded into a data
directory.  

##Basic running of the application on 2010 data
* create a config file: python main.py --createConfig -y 2010 --host localhost --user userName 
--password password --database databaseName --port portNumber -j numberOfThreads --batchRows batchRows
** By default the config file is located in the current working directory. To specify a location, use the -c, --conf flag.
** The -d, --data flag is used to specify the data directory where the downloaded zip files are located. 
* Look at the config file. Make sure that everything in the config file looks right and run the application.
*To run the application type: python main.py
** This will automatically read in the configuration settings and, if everything is correctly configured, it will
put the data into the specified database. 

#Running the application
 For a complete list of options please look at the config file and run python main.py -h.

#What the application does
#Basic SQL
