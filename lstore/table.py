from lstore.index import Index
from lstore.page import Page
from time import time

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