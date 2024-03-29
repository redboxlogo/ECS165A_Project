import pickle
import threading
from lstore.index import Index
from lstore.page import Page
from lstore.locks import ReadWriteLock
from time import time
from uuid import uuid4
from lstore.logger import logger
from collections import defaultdict
from lstore.config import *
import copy
import os
import shutil
import json

class Record:

    def __init__(self, rid, schema_encoding, key, columns):     # default constructor for Record() obj
        self.rid = rid                                          # rid for identification
        self.ridLocStart = None
        self.ridLocEnd = None
        
        self.indirection = None                                 # pointer used to point to tail (if in Base page) or prior update (if in Tail page)
        self.indirectionLocStart = None
        self.indirectionLocEnd = None

        self.startTime = int(time())                                 # record creation time 
        self.startTimeLocStart = None
        self.startTimeLocEnd = None
        
        self.schema_encoding = schema_encoding                  # schema_encoding to keep track of updates
        self.schema_encodingLocStart = None
        self.schema_encodingLocEnd = None
        
        self.key = key                                          # key for dictonary updates
        self.keyLocStart = None
        self.keyLocEnd = None

        self.columns = columns                                  # column data will be empty after writing to page
        self.columnsLoc = []

        self.page_range_indexNUM = None
        self.base_page_indexNUM = None

    def flip_bit(self, byte_str, bit_position):
        byte_int = int(byte_str, 2)  # Convert binary string to integer
        num_bits = len(byte_str)  # Get the number of bits in the byte
        flipped_bit_position = num_bits - 1 - bit_position  # Calculate the position from the right
        flipped_byte_int = byte_int ^ (1 << flipped_bit_position)  # Flip the bit
        flipped_byte_str = format(flipped_byte_int, '0{}b'.format(num_bits))  # Convert back to binary string with leading zeros
        return flipped_byte_str[-num_bits:]  # Cut off any leading zeros added

    def checkIndirection(self):                                 # boolean to check if an indirection exists inside a base record 
        if self.indirection == None:                            # if indirection does not exist  
            return False                                        # return False
        else:                                                   # else if indirection points to something
            return True                                         # return true
        
    def getallCols(self, page):                                 # function to return 
        cols = []                                               # initialize empty column list
        start_index = self.pageLocStart                         # get the start index of the data in page.bytearray
        end_index = self.pageLocEnd                             # get the end index of the data in page.bytearray
        # Iterate over the specified range
        for i in range(start_index, end_index):                 # for loop to go through elements in data
            cols.append(page.data[i])                           # fill cols list with elements
        return cols                                             # return list cols
    
    def setIndirection(self, destinationRec):                   # function sets record.indirection to the input record
        self.indirection = destinationRec                       # set the indirection attribute
        return self                                             # return original record
    
    def getIndirection(self):                                   # get the record stored in the indirection column
        return self.indirection                                 # return record object in indirection
    
    def getSchemaCode(self):                                    # get the schema encoding inside a record
        return self.schema_encoding                             # return the schema encoding
    
    def setSchemaCode(self, schemaCode):                        # change the schema encoding 
        self.schema_encoding = schemaCode                       # change schema encoding to input 
        return self                                             # return the record
    
    def updateBaseCols(self, basePage, newCols):                            # function update data inside base page
        basePage.fill_bytearray(basePage.data, newCols, self.pageLocStart)  # update the Base page with new data
        return self                                                         # return the base record

    def updateTailRec(self, baseRecord, basePage, key, updateCols):                         # function adds new tail record and connects to base and old tail records 

        # print(updateCols)
        tailRID = uuid4().hex                                                               # generate unique RID
        oldTailRecord = baseRecord.getIndirection()                                         # get old tail record
        baseColsList = baseRecord.getallCols(basePage)                                      # get all columns from base record
        updateCols.pop(0)                                                                   # pop the key column off
        newSchema = baseRecord.getSchemaCode()                                              # get schema encoding

        for i in range(len(updateCols)):                                                    # Access each element of the list using the index i
            if(updateCols[i] != None):                                                      # check which columns need updates
                baseColsList[i] = updateCols[i]                                             # update column if update exists
                newSchema[i] += 1                                                           # add to schema (will track the number of times a record is updated)

        newTailRecord = Record(tailRID, newSchema, key, baseColsList)                       # create new tail record for insertion and redirection
        newTailRecord = newTailRecord.setIndirection(oldTailRecord)                         # new tail record points to old tail record

        baseRecord = baseRecord.setIndirection(newTailRecord)                               # base record points to new tail record
        baseRecord = baseRecord.setSchemaCode(newSchema)                                    # set new schema code for base record
        baseRecord = baseRecord.updateBaseCols(basePage, baseColsList)                      # update data(byte array) in base page
        
        # baseRecord.columns = baseColsList
        
        # print(basePage.read_bytearray(basePage.data,baseRecord))

        return newTailRecord
    
    def updatekey(self, newKey):                                                            # update the key of a record
        self.key = newKey                                                                   # make the key attribute equal to newKey input
        return self                                                                         # return recordObj with updated key

    def getStart(self):                                                                     # get the start location for record data in a base page
        return self.pageLocStart                                                            # return start index
    
    def getEnd(self):                                                                       # get the end location for record data in base page            
        return self.pageLocEnd                                                              # return the end index of the record data in base page


class PageRange:
    """
    :param num_columns: int        Number of data columns in the PageRange
    :param parent_key: int         Integer key of the parent Table
    :param pr_key: int             Integer key of the PageRange as it is mapped in the parent Table list
    """
    def __init__(self, num_columns: int, parent_key: int, pr_key: int, parent_path):
        self.table_key = parent_key
        self.num_columns = num_columns
        self.key = pr_key
        self.num_tail_pages = 0
        self.num_tail_records = 0
        self.table_path = parent_path
        self.page_range_path = f"{parent_path}/pageRange{self.key}"  # write name of path for the page range
        os.mkdir(self.page_range_path)  # make the directory

        
        # Default page names
        default_names = ["INDIRECTION","RID", "TIME", "SCHEMA", "KEY"]
        # Generate additional page names
        for i in range(num_columns - 1):
            default_names.append(f"data_column {i + 1}")
        self.base_page = [[Page(name, self.page_range_path) for name in default_names] for _ in range(PAGE_RANGE_SIZE)]
        # self.tail_page = [None] * PAGE_RANGE_SIZE
        tail_names = ["Tail_INDIRECTION","Tail_RID", "Tail_TIME", "Tail_SCHEMA", "Tail_KEY"]  # Template for generating page names
        for i in range(self.num_columns - 1):
            tail_names.append(f"Tail_data_column {i + 1}")
        
        self.tail_page = [[Page(name, self.page_range_path) for name in tail_names] for _ in range(PAGE_RANGE_SIZE)]



        return None



    # Function to fill the None elements with Page objects
    def fill_none_with_Tailpages(self):
        
        name_template = ["Tail_INDIRECTION","Tail_RID", "Tail_TIME", "Tail_SCHEMA", "Tail_KEY"]  # Template for generating page names
        for i in range(self.num_columns - 1):
            name_template.append(f"Tail_data_column {i + 1}")
        
        self.tail_page = [[Page(name) for name in name_template] for _ in range(PAGE_RANGE_SIZE)]


    def capacity_check(self):
        if(self.base_page[-1][RID_COLUMN].num_records == MAX_RECORDS):
            return False
        else:
            return True
        
    def insert_colElement(self,colData:int, page:Page):
        ElementIndex, nextByte = page.fill_bytearray(colData)
        return ElementIndex

    # function to insert information larger than a single byte
    def insert_long(self, primary_key:int , page:Page):
        startIndex, endIndex = page.parse_integer_to_nibbles(primary_key)
        return startIndex, endIndex

    def insert_RID(self, RID:hex , page:Page):
        startIndex, endIndex = page.store_hex_in_bytearray(RID)
        return startIndex, endIndex
    
    def replace_RID(self, RID:hex , page:Page, start:int, end:int):

        return page.store_hex_in_bytearray_by_index(RID,start,end)
    
    def allocate_schema(self, schema:str, page:Page):
        
        numCols = len(schema)

        # Convert the series of zeros to binary
        binary_representation = int(schema, 2)

        # Calculate the number of bytes needed
        num_bytes = (numCols + 7) // 8

        startIndex, endIndex = page.space_allocation(num_bytes)

        # # Convert to bytes
        # byte_value = binary_representation.to_bytes(num_bytes, byteorder='big')


        return startIndex, endIndex
    
    def insert_schema(self, schema:str, page:Page):

        numCols = len(schema)

        # Convert the series of zeros to binary
        binary_representation = int(schema, 2)

        # Calculate the number of bytes needed
        num_bytes = (numCols + 7) // 8

        return
        

    def insert_tailRec(self, newRecord:Record, baseRecord:Record, currTailPage:list):
        
        basePagesNUM = baseRecord.base_page_indexNUM
        tailPage = currTailPage
        basePage = self.base_page[basePagesNUM]

        if(tailPage[RID_COLUMN].num_records == 255):
            print("tail page is full")
            return False
        
        self.num_tail_records +=1 
            
        newRecord.ridLocStart, newRecord.ridLocEnd = self.insert_RID(newRecord.rid, tailPage[RID_COLUMN])      # insert rid into rid column page
        newRecord.startTimeLocStart,newRecord.startTimeLocEnd = self.insert_long(int(newRecord.startTime), tailPage[TIMESTAMP_COLUMN])      # insert start time into time column page
        newRecord.keyLocStart, newRecord.keyLocEnd = self.insert_long(newRecord.key, tailPage[KEY_COLUMN])      # insert key into key column page
        # newRecord.schema_encodingLocStart, newRecord.schema_encodingLocEnd =self.insert_schema(newRecord.schema_encoding, tailPage[SCHEMA_ENCODING_COLUMN])      # insert schema into schema column page)
        
        for i in range(1,self.num_columns-1):
            if(i == 0):
                continue
            # print(baseRecord.columnsLoc[i-1])
            # print(newRecord.columns[i])
            elementIndex = tailPage[KEY_COLUMN+i].fill_bytearray_by_index(baseRecord.columnsLoc[i-1], (baseRecord.columnsLoc[i]+1), newRecord.columns[i])
            newRecord.columnsLoc.append(elementIndex)

        



        # oldTailRID = baseRecord.indirection
        # baseRecord.indirection = newRecord.rid
        # newRecord.indirection = oldTailRID
        self.replace_RID(baseRecord.indirection, basePage[INDIRECTION_COLUMN], baseRecord.indirectionLocStart, baseRecord.indirectionLocEnd)
        self.replace_RID(newRecord.indirection, basePage[INDIRECTION_COLUMN], baseRecord.indirectionLocStart, baseRecord.indirectionLocEnd)
        return newRecord
    
# Each Table should have both Base and Tail pages 

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, path, bufferpool=None):
        self.name = name  # set name of table
        self.table_path = path
        self.key = key  # set table key
        self.num_columns = num_columns  # number of columns
        self.col_names = {
            0: 'Indirection',
            1: 'RID',
            2: 'Timestamp',
            3: 'Schema'
        }
        self.num_records = 0
        self.num_brecords = 0
        self.num_trecords = 0
        self.page_directory = {}  # dictionary given a record key, it should return the page address/location
        self.num_page_ranges = 0
        self.page_range_data = {}
        self.page_range = [PageRange(num_columns=num_columns, parent_key=key, pr_key=0, parent_path = path)]
        self.base_page = []  # list of Base page objects
        self.tail_page = []  # list of Tail page objects
        self.index = Index(self)
        self.record_lock = threading.Lock()
        self.bufferpool = bufferpool
        self.table_path = path
        self.lock_manager = defaultdict()
        

    def __merge(self):
        print("merge is happening")
        pass

    def newPageRange(self):
        mostRecent = self.page_range[-1]
        newRange = PageRange(num_columns=mostRecent.num_columns, parent_key=mostRecent.table_key, pr_key=(mostRecent.key+1))
        self.page_range.append(newRange)
        return newRange
    
    # helper function to set Base record after insert retaining read-only properties of base page
    def setBase(self, basePage, insertRecord):

        try:
            writeSucc = basePage.write(insertRecord)    # try to write to base page

            if(writeSucc == False):                     # if write() failed
                newID = basePage.directoryID-1          # create new basepage ID 
                newBase = self.newPage(newID)           # create new base page
                # logger.info("new Base Page created with ID: {}".format(newID))
                self.base_page.append(newBase)          # append new base page to base page list
                newBase.write(insertRecord)             # write record to new base page
                return newBase                          # return new page object
            else:
                return basePage                         # if write() success, return original base page
        except:
            print("Base setting failed")                # complete page setting failure 
            return False                                # return false

    def newPage(self, pageID):
        returnPage = Page(pageID)                                          # create a base page
        return returnPage                                                  # return newly created base page


    # test edge case of key does not exist
    def getBasePage(self, searchKey):
        try:
            returnPage = self.page_directory.get(searchKey) # get the page containing the location of record
        except:
            print(searchKey)
            print("no page location")
        return returnPage
    

    def setTailPage(self, tailPage, insertRecord, colsList):
        tailRID = uuid4()                                                                            # generate a unique RID for a tail record
        newTailRecord = Record(tailRID, insertRecord.schema_encoding, insertRecord.key, colsList)    # create new record object 
        newTailRecord = newTailRecord.setIndirection(insertRecord)                                   # tail record indirection points to base record
        insertRecord = insertRecord.setIndirection(newTailRecord)                                    # insert record indirection points to 
        try:
            writeSucc = tailPage.write(newTailRecord)   # try to write to base page

            if(writeSucc == False):                     # if write() failed
                newID = tailPage.directoryID+1          # create new tail page ID 
                newTail = self.newPage(newID)           # create new tail page
                # logger.info("new Tail Page created with ID: {}".format(newID))
                self.tail_page.append(newTail)          # append new tail page to tail page list
                newTail.write(newTailRecord)            # write record to new tail page
                return newTail                          # return new page object
            else:
                return tailPage                         # if write() success, return original tail page
        except:
            print("Tail setting failed")                # complete page setting failure 
            return False                                #return false

    def getTailPage(self):
        if(self.tail_page == []):                       # if tail pages do not exist
            newTail = self.newPage(1)                   # create a new tail page with ID 1
            return newTail                              # return new tail page
        else: 
            return self.tail_page[-1]                   # return last tail page in list
        
    def updatePageDirectory(self, newKey, oldKey):
        self.page_directory[newKey] = self.page_directory.pop(oldKey) # Create a new key-value pair with the updated key and value
        return self

    # stores table data in a dictionary for table_directory
    def stores_table_data(self):
        table_data = {
            "name": self.name,
            "key": self.key,
            "table_path": self.table_path,
            "num_columns": self.num_columns,
            "col_names": self.col_names,
            "num_records": self.num_records,
            "num_brecords": self.num_brecords,
            "num_trecords": self.num_trecords,
            "num_page_ranges": self.num_page_ranges,
            "page_range_data": self.page_range_data,
        }
        self.page_directory["table_data"] = table_data

    # fills in table with its specific data
    def fill_in_table_data(self, table_data):
        self.name = table_data["name"]
        self.key = table_data["key"]
        self.table_path = table_data["table_path"]
        self.num_columns = table_data["num_columns"]
        self.col_names = table_data["column_names"]
        self.num_records = table_data["num_records"]
        self.num_brecords = table_data["num_base_records"]
        self.num_trecords = table_data["num_tail_records"]
        self.num_page_ranges = table_data["num_page_ranges"]
        self.page_range_data = table_data["page_range_data"]

    # writes table directory to disk
    def table_page_dir_to_disk(self):
        self.stores_table_data()  # load all the table data
        page_dir = open(f"{self.table_path}/page_directory.pkl", "wb")  # open directory path
        pickle.dump(self.page_directory, page_dir)  # load data into page_directory
        page_dir.close()  # close the directory
        
        return True
