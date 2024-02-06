from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass
        

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # Attempt to find the record for the given primary_key.
        try:
            RID = self.table.index.get(primary_key, None)
            if RID is None:
                return False  # Record does not exist.
        except KeyError:
            return False  # Primary key does not exist in the index.

        location = self.table.page_directory.get(RID)
        if location is None:
            return False  # Location not found, indicating record might not exist.

        # Performs the actual deletion operation here.
        del self.table.page_directory[RID]  # Remove the record from the page directory.
        del self.table.index[primary_key]  # Remove the primary key from the index.

        return True  # Return True upon successful deletion.


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    # helper function to set Base record after insert retaining read-only properties of base page

    def setBase(self, basePage, insertRecord):

        try:
            basePage.write(insertRecord)
            return True
        except:
            print("Base setting failed")
            return False 
        
# insert into both base and tail?

    def insert(self, *columns):

        columns = list(columns)
        RID = columns[0]                                            #temp assignment POSSIBLE CHANGE 
        key = columns[0]                                            #temp assignment POSSIBLE CHANGE 
        schema_encoding = '0' * self.table.num_columns
        newRecord = Record(RID, schema_encoding, key, columns)      #create a new Record() object from table.py
        if (self.table.page_directory == {}):
            newBase = Page(-1)                               #create a base page
            self.setBase(newBase,newRecord)
            self.table.base_page.append(newBase)
            self.table.page_directory.update({newRecord.key:newBase})

        else:
            BaseP = self.table.base_page[-1]
            print(newRecord.columns)
            self.setBase(BaseP,newRecord)
            self.table.page_directory.update({newRecord.key:BaseP})

        pass
        

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
