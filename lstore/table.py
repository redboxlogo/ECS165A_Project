from lstore.index import Index
from lstore.page import Page
from time import time
import math

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, schema_encoding, key, columns):     # default constructor for Record() obj
        self.rid = rid                                          # rid for identification
        self.indirection = None                                 # pointer used to point to tail (if in Base page) or prior update (if in Tail page)
        self.startTime = time()                                 # record creation time 
        self.schema_encoding = schema_encoding                  # schema_encoding to keep track of updates
        self.key = key                                          # key for dictonary updates
        self.columns = columns                                  # column data
        self.pageLocStart = None                                # bytearray index for start of record
        self.pageLocEnd = None                                  # bytearray index for (last element in columns + 1)


# Each Table should have both Base and Tail pages 

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name                                # set name of table
        self.key = key                                  # set table key
        self.num_columns = num_columns                  # number of columns
        self.page_directory = {}                        # dictionary given a record key, it should return the page address/location
        self.base_page = []                             # list of Base page objects
        self.tail_page = []                             # list of Tail page objects
        self.index = Index(self)
        self.key_map_RID = {}                           # dictionary that maps key to RID
        pass

    def __merge(self):
        print("merge is happening")
        pass
        # helper function to set Base record after insert retaining read-only properties of base page

    def setBase(self, basePage, insertRecord):

        try:
            writeSucc = basePage.write(insertRecord)    # try to write to base page

            if(writeSucc == False):                     # if write() failed
                newID = basePage.directoryID-1          # create new basepage ID 
                newBase = self.newBasePage(newID)       # create new base page
                self.base_page.append(newBase)          # append new base page to base page list
                newBase.write(insertRecord)             # write record to new base page
                return newBase                          # return new page object
            else:
                return basePage                         # if write() success, return original base page
        except:
            print("Base setting failed")                # complete page setting failure 
            return False                                #return false

    def newBasePage(self, pageID):
        newBase = Page(pageID)                                          # create a base page
        return newBase                                                  # return newly created base page

    # returns the RID of a record for a specific search key
    # TODO:
    # 1. map RID to specific page location in the database
    # 2. iterate through all base pages and retrieve the key and RID columns
    # 3. check if the entry value matches the given key; if True, get associated RID
    # 4. check if RID is associated with deleted value; if True, return got_RID = None
    # 5. to optimize: check if current base page = last base page; if True, return got_RID
    def get_RID(self, search_key):
        got_RID = None
        pass

    # reads the RID and returns the most recently updated record for that RID
    # TODO:
    # 1. retrieve record info (page_directory?) page range, base page, and page index
    # 2. iterate through all columns of the record and append corresponding entries for the specified page index into all_column_entries
    # 3. extract the key, schema encoding, and user column values from all_column_entries
    # 4. check schema encoding;
            # if schema = 0 (non-updated), return Record object with key, RID, schema encoding, and column values
            # if schema = 1 (updated), retrieve additional information from the tail page directory
    # 5. return a Record object with updated key, RID, schema encoding, and column values
    def read(self, RID):
        # all_column_entries = []
        pass
        
