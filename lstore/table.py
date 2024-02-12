from lstore.index import Index
from lstore.page import Page
from time import time
from uuid import uuid4

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

    def checkIndirection(self):
        if self.indirection == None:
            return False
        else:
            return True
        
    def getallCols(self, page):
        cols = []
        start_index = self.pageLocStart
        end_index = self.pageLocEnd
        # Iterate over the specified range
        for i in range(start_index, end_index):
            cols.append(page.data[i])
        return cols
    
    def setIndirection(self, destinationRec):
        self.indirection = destinationRec
        return self
    
    def getIndirection(self):
        return self.indirection
    
    def getSchemaCode(self):
        return self.schema_encoding
    
    def setSchemaCode(self, schemaCode):
        self.schema_encoding = schemaCode
        return self
    
    def updateBaseCols(self, basePage, newCols):
        basePage.fill_bytearray(basePage.data, newCols, self.pageLocStart)
        return self

    def updateTailRec(self, baseRecord, basePage, key, updateCols):

        # print(updateCols)
        tailRID = uuid4()                                                                   # generate unique RID
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

        
        # print(basePage.read_bytearray(basePage.data,baseRecord))

        return newTailRecord
    
    def updatekey(self, newKey):
        self.key = newKey
        return self

    def getStart(self):
        return self.pageLocStart
    
    def getEnd(self):
        return self.pageLocEnd

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
                newBase = self.newPage(newID)           # create new base page
                self.base_page.append(newBase)          # append new base page to base page list
                newBase.write(insertRecord)             # write record to new base page
                return newBase                          # return new page object
            else:
                return basePage                         # if write() success, return original base page
        except:
            print("Base setting failed")                # complete page setting failure 
            return False                                #return false

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
    

    # this function assumes that a prior tail record does not exist
    def setTailPage(self, tailPage, insertRecord, colsList):
        tailRID = uuid4()
        newTailRecord = Record(tailRID, insertRecord.schema_encoding, insertRecord.key, colsList)
        newTailRecord = newTailRecord.setIndirection(insertRecord)                                   #tail record indirection points to base record
        insertRecord = insertRecord.setIndirection(newTailRecord)                                    #insert record indirection points to 
        try:
            writeSucc = tailPage.write(newTailRecord)   # try to write to base page

            if(writeSucc == False):                     # if write() failed
                newID = tailPage.directoryID+1          # create new tail page ID 
                newTail = self.newPage(newID)           # create new tail page
                self.tail_page.append(newTail)          # append new tail page to tail page list
                newTail.write(newTailRecord)            # write record to new tail page
                return newTail                          # return new page object
            else:
                return tailPage                         # if write() success, return original tail page
        except:
            print("Tail setting failed")                # complete page setting failure 
            return False                                #return false

    def keyUpdate(self):
        pass

    def getTailPage(self):
        if(self.tail_page == []):                       # if tail pages do not exist
            newTail = self.newPage(1)                        # create a new tail page with ID 1
            return newTail                              # return new tail page
        else: 
            return self.tail_page[-1]                   # return last tail page in list
        
    def updatePageDirectory(self, newKey, oldKey):
        self.page_directory[newKey] = self.page_directory.pop(oldKey) # Create a new key-value pair with the updated key and value
        return self