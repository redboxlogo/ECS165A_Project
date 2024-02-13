from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
import logger


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
        # Input Validation: Check if the primary_key exists in the page directory.
        if primary_key not in self.table.page_directory:
            return False  # Return False immediately if primary_key does not exist.

        # Since the primary_key exists, proceed with finding the base page.
        basePage = self.table.getBasePage(primary_key)
        if basePage is None:
            return False  # Return False if the base page does not exist for the given primary key.

        # Use the getRecord method to check if the record exists in the base page's metadata.
        record = basePage.getRecord(primary_key)
        if record is None:
            return False  # Return False if the record does not exist within the base page.

        # Proceed to delete it from the base page's record metadata.
        del basePage.record_metadata[primary_key]

        # Call remove_NumRecords to decrement the number of records in the page after deletion.
        basePage.remove_NumRecords()

        # Remove the record from the page directory.
        del self.table.page_directory[primary_key]

        # Update the index to reflect the deletion.
        # Use self.table.key to reference the primary key column.
        self.table.index.remove(self.table.key, primary_key)

        return True  # Return True upon successful deletion.


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    # need to check for duplicate inserts
    def insert(self, *columns):

        if(self.table.getBasePage(columns[0]) != None):
            print("Record already in directory")
            return False


        columns = list(columns)
        RID = columns[0]                                                # temp assignment POSSIBLE CHANGE 
        key = columns[0]                                                # temp assignment POSSIBLE CHANGE 
        columns.pop(0)                                                  # removing the key
        schema_encoding = [0] * (self.table.num_columns-1)              # assign schema encoding to new records
        newRecord = Record(RID, schema_encoding, key, columns)          # create a new Record() object from table.py
        if (self.table.page_directory == {}):

            BaseP = self.table.newPage(-1)                              # create FIRST base page "-1"
            BaseP = self.table.setBase(BaseP,newRecord)                 # set new record into base page
            self.table.base_page.append(BaseP)                          # append page table of contents (only needs to be done for first page "-1")
            self.table.page_directory.update({newRecord.key:BaseP})     # update page directory with new key and page address
            return True

        else:

            BaseP = self.table.base_page[-1]                            # access the last base_page in the list "-1" DOES NOT REFER TO THE PAGE NUMBER
            BaseP = self.table.setBase(BaseP,newRecord)                 # set new record into base page/ create new base page if first is full
            self.table.page_directory.update({newRecord.key:BaseP})     # update page directory with new key and page address
            return True

        

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
    # Indexes always point to base records, and they never directly point to any tail records, so tail pages must be completed in update.
    """
    def update(self, primary_key, *columns):
        
        try:
            basePage = self.table.getBasePage(primary_key)              # get the base page that contains the record
        except:
            print("key does not exist in directory")
            return False

        updateColumns = list(columns)
        TailPage = self.table.getTailPage()                         # get/create last/new tail record
        try:
            originalRecord = basePage.getRecord(primary_key)            # get the record from the base page
        except:
            print( "page does not exist for this record")
            return False
        baseRecCols = originalRecord.getallCols(basePage)           # get column data of base
        currTable = self.table



        if(updateColumns[0] == -999):                                         # condition for delete record
            
            pass        

        # if(updateColumns[0] != None):                                       # potentially perform a key update
        #     print("updating key")
        #     print("old key:")
        #     print(originalRecord.key)
        #     print("old data")
        #     print(basePage.read_bytearray(basePage.data,originalRecord))
        #     newKey = updateColumns[0]
        #     oldKey = originalRecord.key
        #     originalRecord = originalRecord.updatekey(newKey)
        #     basePage = basePage.updateMetadataKey(newKey,oldKey)
        #     currTable = currTable.updatePageDirectory(newKey,oldKey)
        #     print("new key")
        #     print(originalRecord.key)



        indirectionExists = originalRecord.checkIndirection()                                           # check for pre-existing indirection pointer
        if(indirectionExists == True):                                                                  # if indirection pointer exists, do a pointer redirection
            TailRec = originalRecord.getIndirection()                                                   # if indirection from base record exists, store in "TailRec"
            newTailRec = TailRec.updateTailRec(originalRecord, basePage, primary_key, updateColumns)    # add new tail record and update base page with new tail record
            writeSucc = TailPage.write(newTailRec)                                                      # writeSucc == true if write was successful; false if page is full
            if(writeSucc == False):                                                                     # if write() failed
                newID = TailPage.directoryID-1                                                          # create new basepage ID 
                newTailPage = self.table.newPage(newID)                                                 # create new tail page
                self.table.tail_page.append(newTailPage)                                                # append new tail page to tail page list
                newTailPage.write(newTailRec)                                                           # write record to new tail page
           
            return True
        else:                                                                                               # if indirection pointer doesnt exist
            TailPage = self.table.setTailPage(TailPage, originalRecord, baseRecCols)                        # create first copy of original record in Tail page and return said record
            firstTailRec = originalRecord.getIndirection()                                                  # get first copy of record in Tail page
            newTailRec = firstTailRec.updateTailRec(originalRecord, basePage, primary_key, updateColumns)   # add new tail record and update base page with new tail record
            writeSucc = TailPage.write(newTailRec)                                                          # writeSucc == true if write was successful; false if page is full
            if(writeSucc == False):                                                                         # if write() failed
                newID = TailPage.directoryID-1                                                              # create new basepage ID 
                newTailPage = self.table.newPage(newID)                                                     # create new tail page
                self.table.tail_page.append(newTailPage)                                                    # append new tail page to tail page list
                newTailPage.write(newTailRec)                                                               # write record to new tail page
            # print(TailPage.read_bytearray(TailPage.data,newTailRec))
            return True

    
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
