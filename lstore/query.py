from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.logger import logger
from uuid import uuid4
from lstore.config import *
from time import time
import copy

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
        basePage = self.table.getBasePage(primary_key)
        if basePage is None:
            #logger.info("Delete failed for Record, key does not exist in page metadata, key: {}".format(primary_key))
            return False  # Return False if the base page does not exist for the given primary key.

        # Use the getRecord method to check if the record exists in the base page's metadata.
        record = self.table.index.lookup(primary_key) 

        if record is None:
            #logger.info("Delete failed for Record, key does not exist in page metadata, key: {}".format(primary_key))
            return False  # Return False if the record metadata does not exist within the base page.

        del self.table.index.indices[primary_key]

        # # Call remove_NumRecords to decrement the number of records in the page after deletion.
        # basePage.remove_NumRecords()

        # Remove the record from the page directory.
        # del self.table.page_directory[primary_key]

        # Update the index to reflect the deletion.
        # Use self.table.key to reference the primary key column.
        # self.table.index.remove(self.table.key, primary_key)

        #logger.info("Delete success for Record key: {}".format(primary_key))

        return True  # Return True upon successful deletion.


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # if(columns[0] == 906661711):
        #     print("debug")
        if(self.table.getBasePage(columns[0]) != None):                 #check for duplicates
            print("Record already in directory")
            #logger.info("failed to insert record with key: {}".format(self.table.getBasePage(columns[0])))

            return False


        columns = list(columns)
        RID = uuid4().hex                                               # assign hex RID to record 
        key = columns[0]                                                # extract the key from the input
        columns.pop(0)                                                  # removing the key
        # schema_encoding = bytes()                                       # assign schema encoding as a single empty byte      
        schema_encoding = '0' * (self.table.num_columns-1) 
        
        
        newRecord = Record(RID, schema_encoding, key, columns)          # create a new Record() object from table.py


        recentRange = self.table.page_range[-1]
        if(recentRange.capacity_check() == False):
            recentRange = self.table.newPageRange()


        for basePagesNUM in range(PAGE_RANGE_SIZE):

            if(recentRange.base_page[basePagesNUM][RID_COLUMN].num_records == MAX_RECORDS):
                continue
            
            newRecord.ridLocStart, newRecord.ridLocEnd = recentRange.insert_RID(newRecord.rid, recentRange.base_page[basePagesNUM][RID_COLUMN])      # insert rid into rid column page
            newRecord.startTimeLocStart,newRecord.startTimeLocEnd = recentRange.insert_long(int(newRecord.startTime), recentRange.base_page[basePagesNUM][TIMESTAMP_COLUMN])      # insert start time into time column page
            newRecord.keyLocStart, newRecord.keyLocEnd = recentRange.insert_long(newRecord.key, recentRange.base_page[basePagesNUM][KEY_COLUMN])      # insert key into key column page
            newRecord.schema_encodingLocStart, newRecord.schema_encodingLocEnd = recentRange.allocate_schema(schema_encoding, recentRange.base_page[basePagesNUM][SCHEMA_ENCODING_COLUMN])      # insert schema into schema column page)
            recentRange.base_page[basePagesNUM][INDIRECTION_COLUMN].num_records += 1
            recentRange.base_page[basePagesNUM][INDIRECTION_COLUMN].nextDataBlock = recentRange.base_page[basePagesNUM][RID_COLUMN].nextDataBlock
            newRecord.indirectionLocStart = newRecord.ridLocStart
            newRecord.indirectionLocEnd = newRecord.ridLocEnd

            newRecord.page_range_indexNUM = recentRange.key
            newRecord.base_page_indexNUM = basePagesNUM

            for i in range(self.table.num_columns-1):
                elementIndex = recentRange.base_page[basePagesNUM][KEY_COLUMN+i+1].parse_integer_to_nibbles(newRecord.columns[i])
                newRecord.columnsLoc.append(elementIndex)
            # return self.table.index.insert_newrec(newRecord)

            self.table.page_directory.update({newRecord.key:recentRange.base_page[basePagesNUM]})     # update page directory with new key and page address
            
            self.table.index.insert_newrec(newRecord)

            return True


        return True
        # logger.info("new record inserted to Base Page with key: {}".format(newRecord.key))


        

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
        records_list = []  # initialize list of Record objects to return
        data_list = []  # initialize list 

        current_page = self.table.getBasePage(search_key)  # gets current, most updated base page
        current_Rec = self.table.index.lookup(search_key)  # gets current, most updated base page
        currRange = self.table.page_range[current_Rec.page_range_indexNUM]
        first_column = projected_columns_index.pop(0)  # first column value (0 or 1)

        if(current_Rec.indirection != None):
            # print(current_bRec.indirection)
            # print(current_Rec)
            current_Rec = self.table.index.lookup_tail(current_Rec.indirection)
            # print(current_Rec)
            current_page = self.table.page_range[current_Rec.page_range_indexNUM].tail_page[current_Rec.base_page_indexNUM]
            current_Rec.columns.pop(0)
            


        # if 0, append nothing; if 1, we return the key of the record metadata
        if first_column == 0:
            data_list.append(None)
        else:
            data_list.append(current_Rec.key)

        # print(projected_columns_index)

        for i in range(len(projected_columns_index)):  # loop through the data
            # print(projected_columns_index[i])
            if projected_columns_index[i] == 1:

                # print(current_page[KEY_COLUMN+i+1].directoryID)
                # print(current_Rec.columnsLoc[i])
                # print(current_page[KEY_COLUMN+i+1].data[current_Rec.columnsLoc[i]])
                # data_list.append(current_page[KEY_COLUMN+i+1].data[current_Rec.columnsLoc[i]])                      ################################## should read from page
                data_list.append(current_Rec.columns[i])
            else:
                data_list.append(None)  # otherwise, we append None

        # print(data_list)
        current_Rec.columns = data_list # puts everything from data_list into the columns of the record
        records_list.append(current_Rec)  # append record metadata and return
        # logger.info("selecting Record with key: {}".format(current_record_metadata.key))
        return records_list
    
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

        # print(primary_key)
        
        try:
            basePage = self.table.getBasePage(primary_key)              # get the base page that contains the record
        except:
            print("failed to get base page")
            # logger.info("Update failed for Record, key does not exist in page directory, key: {}".format(primary_key))
            return False
        
        if(basePage == None):
            print("key does not exist in directory")

        updateColumns = list(columns)
        baseRecordObj = self.table.index.lookup(primary_key)
        # print(baseRecordObj.base_page_indexNUM)
        currRange = self.table.page_range[baseRecordObj.page_range_indexNUM]
        fullTailRange = currRange.tail_page[baseRecordObj.base_page_indexNUM]
        currTailPage = fullTailRange[:(self.table.num_columns+4)]
        # currTailPageIndex = (self.table.num_columns+4)


        # check capacity of tail page
        while(currTailPage[RID_COLUMN].num_records == MAX_RECORDS):

            tail_names = ["Tail_INDIRECTION","Tail_RID", "Tail_TIME", "Tail_SCHEMA", "Tail_KEY"]  # Template for generating page names
            for i in range(self.table.num_columns - 1):
                tail_names.append(f"Tail_data_column {i + 1}")
            currTailPage = [Page(name) for name in tail_names]
            currRange.tail_page[baseRecordObj.page_range_indexNUM].extend(currTailPage)


                


        indices_not_none = [index for index, value in enumerate(updateColumns) if value is not None]


        if(baseRecordObj.indirection == None):
            FIRSTTailRecord = copy.deepcopy(baseRecordObj)
            FIRSTTailRecord.rid = uuid4().hex
            FIRSTTailRecord.indirection = baseRecordObj.rid
            baseRecordObj.indirection = FIRSTTailRecord.rid
            self.table.index.insert_tailrec(FIRSTTailRecord)
            #insert_tailRec does not handle indirection rearrangement
            FIRSTTailRecord = currRange.insert_tailRec(FIRSTTailRecord, baseRecordObj,currTailPage)

        # if(primary_key == 906660054):
        #     print("debug")

        # check capacity of tail page
        while(currTailPage[RID_COLUMN].num_records == MAX_RECORDS):

            tail_names = ["Tail_INDIRECTION","Tail_RID", "Tail_TIME", "Tail_SCHEMA", "Tail_KEY"]  # Template for generating page names
            for i in range(self.table.num_columns - 1):
                tail_names.append(f"Tail_data_column {i + 1}")
            currTailPage = [Page(name) for name in tail_names]
            currRange.tail_page[baseRecordObj.page_range_indexNUM].extend(currTailPage)


        # print(self.table.index.lookup_tail(baseRecordObj.indirection).key)
        recentTailRec = self.table.index.lookup_tail(baseRecordObj.indirection)

        newTail = copy.deepcopy(recentTailRec)
        newTail.startTime = int(time())
        newTail.schema_encoding = '0' * (len(columns)-1) 
        newTail.indirection = recentTailRec.rid
        newTail.rid = uuid4().hex

        baseRecordObj.indirection = newTail.rid

        if(updateColumns[0] != None):                                       # potentially perform a key update
            pass
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

        for i in indices_not_none:

            newTail.columns[i] = updateColumns[i]
            newTail.schema_encoding = newTail.flip_bit(newTail.schema_encoding, i-1)


        newTail = currRange.insert_tailRec(newTail, baseRecordObj, currTailPage)
        self.table.index.insert_tailrec(newTail)
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_key, end_key, aggregate_column_index):
        sumList = []
        # print(aggregate_column_index)
        aggregate_column_index -= 1
        found_valid_data = False  # Flag to track if any valid data is aggregated

        for i in range(start_key, end_key + 1):

            basePage = self.table.getBasePage(i)

            if basePage is None:
                print("cant find page")
                return False

            # Use the getRecord method to access the record
            record = self.table.index.lookup(i) 


            if record is None:
                print(f"Can't find record with key: {i}")
                sumList.append(0)  # Reflect missing record in aggregation
                continue  # Proceed to the next key in the range

            # print(basePage[KEY_COLUMN+1+aggregate_column_index].directoryID)

            # A valid record is found, process it
            found_valid_data = True
            if aggregate_column_index == -1:
                sumList.append(record.key)
            else:
                value = basePage[KEY_COLUMN+1+aggregate_column_index].read_byte_by_index(record, (aggregate_column_index+1))
                sumList.append(value)

        # After processing the range, check if any valid data was aggregated
        if not found_valid_data:
            return False  # No valid data found in the entire range

        # Return the total sum of aggregated data
        return sum(sumList)

    
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
