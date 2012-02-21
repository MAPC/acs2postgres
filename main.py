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
 """