import openpyxl
import os, sys
import logging
from DBOps import DBOps

FILE="/home/ben/workspace/MAPC_DevtDatabase_V1_3_21_11.xlsx"

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



setupLog("./db_from_xls.log", 3)
myDBOps = DBOps("localhost", 5432, "development", "ben", "password")
book = openpyxl.load_workbook(FILE)
sheet = book.get_sheet_by_name("MAPC_DevtData_V1_3_21_11")
firstRow = True
headers = []
data = []
header_dict = {}
for row in sheet.rows:
    data_row = []
    for cell in row:
        if firstRow == True:
            headers.append(cell.value)
        else:
            value = ""
            if cell.value != None:
                value = "%s" % cell.value
            data_row.append(value)
    if firstRow == True:
        firstRow = False
    else:
        #print data_row[45]
        header_dict = myDBOps.colTypeFromArray(data_row, header_dict)
        data.append(data_row)

db_headers = []
for x in range(0, len(headers)):
    db_headers.append((headers[x], header_dict[x]))
#print header_dict
myDBOps.createTable("migrate_table", db_headers[0], None, db_headers[1:])
myDBOps.insert("migrate_table", headers, data)

    
    

