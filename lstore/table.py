from lstore.index import Index
from lstore.page import Page
from time import time
import math

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

# idk if we should put this in page?
PAGE_SIZE = 4096
BYTES_PER_ELEMENT = 8
PAGES_PER_PRANGE = 16

ELEMENTS_PER_PAGE = PAGE_SIZE / BYTES_PER_ELEMENT
RECORDS_PER_PRANGE = PAGES_PER_PRANGE * ELEMENTS_PER_PAGE


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

    def get_record_cols(self, search_key, baseRID):
        base_index = self.get_page_index(baseRID)  # get base page index
        base_offset = self.get_page_offset(baseRID)  # get base page offset
        base_record = self.base_page[base_index].get_record(base_offset)  # fetch the record
        base_pointer = base_record[INDIRECTION_COLUMN]  # get base indirection RID
        schema = base_record[SCHEMA_ENCODING_COLUMN]  # retrieves schema encoding info stored in the col

        if schema == 1:  # retrieves from tail page if schema = 1
            tail_index = self.get_page_index(base_pointer)  # get tail page index
            tail_offset = self.get_page_offset(base_pointer)  # get tail page offset
            tail_record = self.tail_page[tail_index].get_record(tail_offset)  # fetch the record
            record = Record(tail_record[RID_COLUMN], schema, search_key, tail_record[4:])
        else:  # retrieves from base page if schema != 1
            record = Record(base_record[RID_COLUMN], schema, search_key, base_record[4:])
        return record

    # calculate where RID is located in base pages
    def get_page_index(self, RID):
        return math.floor((RID % (PAGES_PER_PRANGE * ELEMENTS_PER_PAGE)) / ELEMENTS_PER_PAGE)

    # count bytes until data starts
    def get_page_offset(self, RID):
        offset = RID
        while offset >= RECORDS_PER_PRANGE:
            offset -= RECORDS_PER_PRANGE
        while offset >= ELEMENTS_PER_PAGE:
            offset -= ELEMENTS_PER_PAGE
        return offset

    def get_record(self, offset):  # still need to work on this function, below is the pseudocode
        record = []
        # loop through each metadata column and read data at the given offset:
            # read the data in the metadata column (where to access in metadata?)
            # append data to record
        # loop through each data column and read data at the given offset:
            # read the data in the data column
            # append data to record
        # return record

    def select(self, search_key, search_key_index, projected_query_columns):
        if search_key not in self.key_map_RID:
            print("RID does not exist for this key")
            return False
        baseRID = self.key_map_RID[search_key]
        current_prange = math.floor(baseRID / RECORDS_PER_PRANGE)  # identify page range we are looking at
        record = self.page_directory[current_prange].select(search_key, baseRID)
        return [record]
        
