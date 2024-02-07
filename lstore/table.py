from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, schema_encoding, key, columns):
        self.rid = rid
        self.indirection = None
        self.startTime = time()
        self.schema_encoding = schema_encoding
        self.key = key
        self.columns = columns
        self.pageLocStart = None
        self.pageLocEnd = None


# Each Table should have both Base and Tail pages 

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}                        #given a record key, it should return the page address/location
        self.base_page = []
        self.tail_page = []
        self.index = Index(self)
        pass

    def __merge(self):
        print("merge is happening")
        pass
        # helper function to set Base record after insert retaining read-only properties of base page

    def setBase(self, basePage, insertRecord):

        try:
            writeSucc = basePage.write(insertRecord)

            if(writeSucc == False):
                newID = basePage.directoryID-1
                newBase = self.newBasePage(newID)
                newBase.write(insertRecord)
                return True
            else:
                return True
        except:
            print("Base setting failed")
            return False 

    def newBasePage(self, pageID):

        newBase = Page(pageID)                                          #create a base page
        return newBase