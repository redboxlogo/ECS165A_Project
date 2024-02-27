from lstore.logger import logger

PAGE_SIZE = 4096

class Page:

    def __init__(self, directoryID):
        self.directoryID = directoryID          # page identifier: positive for tail pages; negative for base
        self.num_records = 0                    # count total records
        self.record_metadata = {}               # dictionary of records
        self.data = bytearray(PAGE_SIZE)        # page size/data
        self.nextDataBlock = 0                  # page index for next empty area of page

    def has_capacity(self):                     # check remain capacity of page
        return PAGE_SIZE-self.nextDataBlock     # return total remaining capacity

    def remove_NumRecords(self):               # remove record from page
        self.num_records -= 1                  # decrement record count

    def convertIntToString(self, inputINT):
        retString = str(inputINT)
        return retString
        

    def fill_bytearray(self, byte_array, value_list, startloc):                         # function used to fill column data into bytearray()
        start_index = startloc                                                          # get start location for write op (append)
        last_index = min(start_index + len(value_list), len(byte_array))                # get end index for write op
        for i in range(start_index, last_index):                                        # for loop to fill byte_array
            byte_array[i] = value_list[i - start_index]   
        return start_index, last_index                                                  # return the start and last index

    def read_bytearray(self,recordObj):                                                 # read data inside page bytearray()
        returnData = []                                                                 # initialize returnData list containing the data
        byte_array = self.data
        start_index = recordObj.getStart()                                              # get the start index of data inside bytearray
        last_index = recordObj.getEnd()                                                 # get the end index of data inside bytearray
        for i in range(start_index, last_index):                                        # for loop to read byte_array
            returnData.append(byte_array[i])                                            # full returnData list
        return returnData                                                               # return the returnData list
    
    def read_byte_by_index(self, recordObj, column):                                    # read data inside page bytearray() for a specified column
        bytearr = self.data
        returnData = []
        start_index = recordObj.getStart()                                              # get the start index of data inside bytearray
        last_index = recordObj.getEnd()                                                 # get the end index of data inside bytearray
        returnval = bytearr[start_index + column] 
        for i in range(start_index, last_index):                                        # for loop to read byte_array
            returnData.append(bytearr[i])                                               # full returnData list
        return returnval                                                                # return the returnData list

    def recordColDel(self, RecordObj):                                                  # delete column data from record object after writing to byte array
        RecordObj.columns = None
        return RecordObj
    
    # checks capacity of page
    # writes to bytearray
    # update page dirctionary with metadata
    # move next datablock
    def write(self, RecordObj):
        remainCapacity = self.has_capacity()                                                                                        # get remaining capacity of page
        dataAmmount = len(RecordObj.columns)                                                                                        # get length of data that needs to be written

        if(remainCapacity < dataAmmount):                                                                                           # if page doesnt have enough space for data abort and return false
            return False
        else:
            self.num_records += 1                                                                                                   # if remaining capacity is fine continue
            RecordObj.colLocStart, RecordObj.colLocEnd = self.fill_bytearray(self.data, RecordObj.columns, self.nextDataBlock)    # fill the byte array with data and return the (first element location) and (last element location +1)
            self.record_metadata.update({RecordObj.key:self.recordColDel(RecordObj)})                                               # store dataless metadata for record and clear record.columns with recordColDel()
            self.nextDataBlock = RecordObj.pageLocEnd                                                                               # update write head with new location
            return True
        
    # test edge case of key does not exist
    def getRecord(self, searchKey):
        try:
            returnRecord = self.record_metadata.get(searchKey) # get the page containing the location of record
        except:
            print("no Record location")
            return False
        return returnRecord
        
    def updateMetadataKey(self, newKey, oldKey):
        self.record_metadata[newKey] = self.record_metadata.pop(oldKey) # Create a new key-value pair with the updated key and value
        return self


    '''
    reads pages from disk
    '''

    def read_from_disk(self, file_path, column):
        file = open(file_path, "rb")  # opens file in binary read mode
        file.seek(column * PAGE_SIZE)  # moves the file pointer to the specified position
        self.data = bytearray(
            file.read(PAGE_SIZE))  # reads a page-sized chunk of data from the file and stores into bytearray
        file.close()  # close the file

        return True


"""
writes pages to disk for the file path
"""


def write_to_disk(file_path, all_cols):
    file = open(file_path, "wb")  # opens file in binary write mode
    for i in range(len(all_cols)):  # iterates over each column in the all_cols list
        file.write(all_cols[i].data)  # writes the data of each column to the opened binary file
    file.close()  # close file
