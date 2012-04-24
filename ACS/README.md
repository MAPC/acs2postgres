# American Community Survey conversion script

This application takes [American Community survey (ACS)](http://www.census.gov/acs/) data and inserts it into a database. The data can be downloaded at
[ftp://ftp.census.gov/acs2010_5yr/](ftp://ftp.census.gov/acs2010_5yr/). This is the 2010 5 year data. Other ACS data can be found by looking at the ACS folders
under [ftp://ftp.census.gov/](ftp://ftp.census.gov/). This introduction will guide the user in how to setup
the environment and a basic series of steps that should be performed in a specific order. Section 2 will go over what actions are
performed and how. The last section will have some brief SQL statements that will be useful for creating additional
views from the data.

To see the help run: 

    $ python main.py -h

## Initial setup

The initial requires a bit of work. This application does not create a database or a database user. This 
must be specified in advance. The user permissions should be fairly restrictive or a temporary user
because the password has to be stored in the configuration file. The user will have to be able to
create tables, drop tables, create views, drop views, and add rows to a table. If the user is not given
drop table privileges, unexpected results will happen but most likely everything will fail due to primary
key constraint issues.  

The zip files should be downloaded and placed in the `data` folder. The data folder is assumed to be
`/current working dir/data`. This can be changed with the -d, --data option when running the application. 
The zip files are found as zip1 and zip2 in the configuration files. Make sure that the names are correct
as these names are a best guess (though correct for 2010 5 year data). There should be only three zip files
in the data directory of the form YYYY_SummaryFileTemplates.zip, STATENAME_All_Geo..., 
and STATENAME_Tract_Block.... these zip files will be unziped in the data directory into a folder named
the zip file. 

*A quick racap:* A database and database user need to be created and the zip files downloaded into a data
directory. The data directory can be specified with the -d, --data option. This is the only setup required
for for this application to run. 

## Basic running of the application on 2010 data

Create a config file:  

    $ python main.py --createConfig -y 2010 --host localhost --user userName --password password --database databaseName --port portNumber -j numberOfThreads --batchRows batchRows

By default the config file is located in the current working directory. To specify a location, use the `-c, --conf` flag.
The `-d, --data` flag is used to specify the data directory where the downloaded zip files are located. 

Look at the config file. Make sure that everything in the config file looks right and run the application.

To run the application type: 

    $ python main.py

This will automatically read in the configuration settings and, if everything is correctly configured, it will
put the data into the specified database. 

# Options for running the application

    -h, --help              show this help message and exit
      
    --createConfig          This will create the configuration file. If the --conf
                            flag is not passed the default file location and name
                            will be used. --year needs to be passed as a parameter
                            if this option is used.
                            
    -y YEAR, --year YEAR    This is the year to get data for as YYYY format. This
                            is *required* for the createConfig parameter and is used
                            as the base year for quite a lot of file names. For
                            now, the only 5 year data will be retrieved. This
                            might be expanded later. *There is no default for this
                            option.*
                            
    --host HOST             This is the database host to connect to. --host is
                            *required* when the --createConfig flag is passed.
                            
    --port PORT             This is the database port to connect to. The default
                            is 5432.
                            
    -u USER, --user USER    This is the user to login with. -u or --user must be
                            passed when the --createConfig flag is passed. There
                            is no default.
                            
    --password PASSWORD     This is the password to login with. If the password is
                            not passed when --createConfig is passed the password
                            will be empty. There is no default.
                            
    -b DATABASE, --database DATABASE 
                            This is the database to connect to on the host. -b or
                            --database is *required* when the --createConfig flag
                            is passed. There is no default.
                            
    -d DATA, --data DATA    This is the path to the data directory. Please make
                            sure that you have read AND write access to whichever
                            directory is chosen. This will be the download
                            directory for all of the data and the unpack location.
                            The default is /current working directory/data
                            
    -c CONF, --conf CONF    This is the path to the configuration file used. This
                            will default to '/current working directory/acs.conf'.
                            The current working directory is most likely the
                            directory which this application is run from. Please
                            make sure that you have read AND write access to
                            whichever directory is chosen. This will also be the
                            root directory used in the configuration file.
                            
    -l LOG, --log LOG       This is the path for the log file. Please make sure
                            that you have read AND write access to whichever
                            directory is chosen. The default is /current working
                            directory/acs.log
                            
    -v, --verbose           Defines the log level. The more v's the finer the
                            logging. The default is to log at the info level. -v:
                            ERROR and CRITICAL -vv: WARNING -vvv: INFO -vvvv:
                            DEBUG
                            
    -j NUMTHREADS, --numThreads NUMTHREADS 
                            This defines the max number of threads to use while
                            running this program. The more threads, the more
                            speadup but the more possible thrashing. A good
                            Typical value is n+1 where n is the number of cores in
                            the computer running the application. More can be used
                            for tuning the application. The default value is 2.
                            
    --batchRows BATCHROWS   This defines the number of rows to batch before
                            writing to the database. The more rows the faster the
                            application can run but the more ram required. Tune
                            this parameter to find the fastest speed for a
                            particular computer. The default value is 200
                            
    --debug                 Adding this flag will log at the debug level and only
                            insert data from 1 e* and m* file and only 1 geo file.
                            This is to test everything to make sure it all works.
                            This is NOT written to the config file and needs to be
                            invoked EVERY time it is needed.

Important options are *-j*, *--batchRows*, and *--data*. The number of threads and the number of rows to batch
commit is quite important for a tuning capacity. The number of cores in your processor + 1 is generally 
recommended for the -j. There is no good value for batchRows but more batched rows requires more RAM. It is recommended
that that the user experiment to find which value is quickest using the --debug option, or at least an acceptable
number. Remember that each line is between 10 and 15 table's worth of data and 10 to 15 commits happen for each
batch job. 

# What the application does

This application will read in ACS data when the zipped data is placed correctly into the data folder. This application
was tested with these three files:

* 2010_SummaryFileTemplates.zip
* Massachusetts_All_Geographies_Not_Tracts_Block_Groups.zip
* Massachusetts_Tracts_Block_Groups_Only.zip

This should work with any file as long as it is specified correctly in the config file. Note that the state option
is not a parameter. This would be a simple extension but one that the author did not implement. Currently it is
hard coded into the application.
  
There is also the issue of the FTP. The original intent was to download the three files automatically and run this
program. This proved infeasible since the structure of the data is inconsistent between different years. Indeed, since
the user is forced to download the correct files, it makes the program much easier to understand as well. This might
have been a bigger issue but the data changes once a year. If this is going to be used more frequently, perhaps 
setting up a cron job to check if the files are any different and if so, download them and run this application would
be in order. 

This application will automatically generate its own config file. Generating the config file requires more
parameters to be passed. All can be overridden when running the application. The config file can be generated once
and stored with the parameters changed. The structure cannot be altered, however.

# Database Structure
Once the application has been successfully run, the database will be populated from all of the e* and m* files. The geography tables will be populated from the g* files. 

There are many tables in the e* and m* files. If one chooses to inspect the e* and m* files one will find that the column names are of the format (table name)_(number). There can be many tables in the same file, each with their own number.

This structure is put into the database as many tables structured as (table name)\_e, (table name)\_m and (table name)\_meta. The meta-table contains human readable column names for that particular table. For example it will map column \_001 to "Total...". The same thing, more or less, happens with the geofile. The script will also automatically create a view called (table name) which will alternate the e* columns and the m* columns with names (column name), (column name)\_error respectively. 

*No other views are created automatically.*

# Basic SQL

`/* */` is the syntax for block comments.  
`--` will specify a comment to the end of the line. 
  
The `CREATE VIEW` syntax is defined at the [PostgreSQL docs](http://www.postgresql.org/docs/9.0/static/sql-createview.html).

Views are tables which are generated from select statements. The most basic view (called View_Name) is created by running:
  
    CREATE VIEW View_Name AS Select * from Some_Table
  
Column names can be added as aliases in either the select statement of the view statement. This is
important to understand because a view does not exist without the underlining tables but will update
upon the data being updated, inserted, or deleted. This eliminates data duplication while still being
able to select specific table columns to generate a new "table" (view). 

The `SELECT` statement can be quite complicated. It is described in detail at the [PostgreSQL docs](http://www.postgresql.org/docs/9.0/static/sql-select.html). 

When creating a view from a select statement, the select statement must be deterministic. This means that no variables are allowed in the select statement which generates the view as this is getting into scripting. Also, the `SELECT SUM()`
will sum together the whole column. The addition operator `+` will be used to sum together columns for
a row. If one of the elements added together is NULL, the expression is NULL.
  
These select statements could become quite complicated. The `if` statement in a select is
sort of handled by a `CASE WHEN ... THEN ... (ELSE) ... END` statement, described at the 
[PostgreSQL docs](http://www.postgresql.org/docs/9.0/static/functions-conditional.html). 
  
The next thing to mention are joins. Joins will link up a mapping between two tables on what should be a key.
It doesn't have to be a key, but the column(s) should be both an integer and a key or key group. Never key off
of text as " " != "" and "A"!="a". This causes data to be prone to errors. If there are multiple selections posible, 
create a new table and make an ID the primary key with a text value. The syntax of a join is:
  
    From Table 1 (table 1 alaias ex. t1)
    JOIN Table2 (table 2 alias ex. t2) on t1.(key column) = t2.(key column)`  

A specific column can be refered to as (table 1 aliais).(column name) ex. t1.ID, t2.ID. For example, to select the 
"ID" column from table 1 and 2, the "name" column from table 1 and the "date" column from table 2 run:

    SELECT t1.ID, t2.ID, t1.name, t2.date
    from Table1 t1
    join Table2 t2 on t1.ID = t2.ID`
   
Much more complicated expressions can exist such as embedding a select statement into the join but 
those are the basics.

## Code example
  
The case statement must be deterministic as well, so no variable aliasing is allowed. Take this 
very nasty bit of code:

    CASE
    WHEN (e._011 + e._022) is NULL then -.99999

    WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
    e._008 + e._009 + e._010 + e._011 + e._014 + 
    e._015 + e._016 + e._017 + e._018 + e._019 + 
    e._020 + e._021 + e._022) is NULL then -.99999

    when (e._011 + e._022)=0 then 0

    when (e._003 + e._004 + e._005 + e._006 + e._007 + 
    e._008 + e._009 + e._010 + e._011 + e._014 + 
    e._015 + e._016 + e._017 + e._018 + e._019 + 
    e._020 + e._021 + e._022) = 0 then 0

    when /* (1/OWNOCCV2) * SQRT(CB_50_ME^2-(CB_50^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
    (power(m._011, 2) + power(m._022, 2)) 
    > 
    (
    power(e._011 + e._022, 2) 
    / 
    power(e._003 + e._004 + e._005 + e._006 + e._007 + --OWNOCCV2^2
    e._008 + e._009 + e._010 + e._011 + e._014 + 
    e._015 + e._016 + e._017 + e._018 + e._019 + 
    e._020 + e._021 + e._022, 2)
    )*
    (power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --OWNOCCV2ME^2
    power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
    power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
    power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
    power(m._021, 2)+ power(m._022, 2)) 
    then
    (100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
    e._008 + e._009 + e._010 + e._011 + e._014 + 
    e._015 + e._016 + e._017 + e._018 + e._019 + 
    e._020 + e._021 + e._022)) *

    SQRT( (power(m._011, 2) + power(m._022, 2)) - 

    (power(e._011 + e._022, 2) 
    / 
    power(e._003 + e._004 + e._005 + e._006 + e._007 + 
    e._008 + e._009 + e._010 + e._011 + e._014 + 
    e._015 + e._016 + e._017 + e._018 + e._019 + 
    e._020 + e._021 + e._022, 2)) *

    (power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
    power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
    power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
    power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
    power(m._021, 2)+ power(m._022, 2)))

    else /* (1/OWNOCCV2) * SQRT(CB_ME^2+(CB^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
    (100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
    e._008 + e._009 + e._010 + e._011 + e._014 + 
    e._015 + e._016 + e._017 + e._018 + e._019 + 
    e._020 + e._021 + e._022)) *

    SQRT( (power(m._011, 2) + power(m._022, 2)) + 

    (power(e._011 + e._022, 2) 
    / 
    power(e._003 + e._004 + e._005 + e._006 + e._007 + 
    e._008 + e._009 + e._010 + e._011 + e._014 + 
    e._015 + e._016 + e._017 + e._018 + e._019 + 
    e._020 + e._021 + e._022, 2)) *

    (power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
    power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
    power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
    power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
    power(m._021, 2)+ power(m._022, 2)))

    END as CB_50_ME_P

Let's go over this (more or less) line by line. `CASE` defines the code block. 
`WHEN (e._011 + e._022) is NULL then -.99999` describes that if either e._011 or e._022 is NULL then display
-.99999 as the value for that column.  This is the same thing for the next `WHEN` statement.
  
`when (e._011 + e._022)=0 then 0` will display 0 if both columns sum together to form 0 for a particular row. 
The next when is analogous to this.

The following code describes this equation: (1/OWNOCCV2) * SQRT(CB_50_ME^2-(CB_50^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100

    when /* (1/OWNOCCV2) * SQRT(CB_50_ME^2-(CB_50^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
    (power(m._011, 2) + power(m._022, 2)) 
    > 
    (
    power(e._011 + e._022, 2) 
    / 
    power(e._003 + e._004 + e._005 + e._006 + e._007 + --OWNOCCV2^2
    e._008 + e._009 + e._010 + e._011 + e._014 + 
    e._015 + e._016 + e._017 + e._018 + e._019 + 
    e._020 + e._021 + e._022, 2)
    )*
    (power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --OWNOCCV2ME^2
    power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
    power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
    power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
    power(m._021, 2)+ power(m._022, 2))`

    This test is making sure that the square root is positive. The column names are defined in the rest of the table as:
    * CB_50_ME^2: `(power(m._011, 2) + power(m._022, 2))`
    * CB_50^2: `power(e._011 + e._022, 2)`
    *OWNOCCV2^2: `power(e._003 + e._004 + e._005 + e._006 + e._007 + --OWNOCCV2^2
      e._008 + e._009 + e._010 + e._011 + e._014 + 
      e._015 + e._016 + e._017 + e._018 + e._019 + 
      e._020 + e._021 + e._022, 2)`
    * OWNOCCV2ME^2: `power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --OWNOCCV2ME^2
      power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
      power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
      power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
      power(m._021, 2)+ power(m._022, 2)
 
Note though that the column names can not be used because the column names have not been created yet. All
of the column names are created at the same time in the view. This is why this can get very complicated with
complex column definitions and manipulations. The `then` statement is the actual function computed. Note the
distinct lack of variables or aliases. 