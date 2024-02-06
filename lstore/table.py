from lstore.index import Index
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
        self.page_directory = {}
        self.base_page = []
        self.tail_page = []
        self.index = Index(self)
        pass

    def __merge(self):
        print("merge is happening")
        pass
 
