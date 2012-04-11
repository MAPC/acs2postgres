import logging
import  Queue, threading
from zipfile import ZipFile

#TODO: make this also zip up various files, perhaps using threads for different zip files
"""
The zipFile class works with the TheadedUnzip to unzip files.
The toDir should be the directory to unzip to, passing None
will create a folder with the name of the zip file and
extract to there.

format of toDir should be in /path/to/zip/zipfile.zip format
"""

class zipFile:
    def __init__(self, zipfile, toDir=None):
        self.zipfile = zipfile
        self.toDir = toDir

"""
This class unzips a zipFile. Note that the zipFile class
should be the object type for the queue.
"""
class ThreadUnzip(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
    
    def unzip(self, from_zip, toDir):
        zf = ZipFile(from_zip, "r")
        zf.extractall(toDir)
        zf.close()
        
    def run(self):
        #This is always true because the queue.get command will raise an exception if empty 
        while True:
            zipFile = self.queue.get()
            logging.info("Unzipping %s" % zipFile.zipfile)
            
            unzip_loc = None
            if zipFile.toDir == None:
                unzip_loc = zipFile.zipfile.split(".")[0]
            else:
                unzip_loc = zipFile.toDir
            self.unzip(zipFile.zipfile, unzip_loc)
            
            
            logging.info("Finished unzipping %s" % zipFile.zipfile)
            self.queue.task_done()