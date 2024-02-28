from lstore.logger import logger
from lstore.config import *



class Page:
    # each page should only store 256 records

    def __init__(self, directoryID: str):
        self.directoryID = directoryID          # page identifier: positive for tail pages; negative for base
        self.num_records = 0                    # count total records
        self.record_metadata = {}               # dictionary of records
        self.data = bytearray(PAGE_SIZE)        # page size/data
        self.nextDataBlock = 0                  # page index for next empty area of page

    def has_capacity(self):                     # check remain capacity of page
        return PAGE_SIZE-self.nextDataBlock     # return total remaining capacity

    def remove_NumRecords(self):               # remove record from page
        self.num_records -= 1                  # decrement record count
        
    def fill_bytearray(self, value):                         # function used to fill column data into bytearray()
        start_index = self.nextDataBlock                                                          # get start location for write op (append)
        self.data[start_index] = value

        self.nextDataBlock += 1
        self.num_records += 1
        return start_index                                                  # return the start and last index

    def fill_bytearray_by_index(self,start, end, new_value):
        startIndex = start
        endIndex = end

        for i in range(start, end):
            self.data[i] = new_value

        return None

    def read_bytearray(self,recordObj):                                    # read data inside page bytearray()
        returnData = []                                                                 # initialize returnData list containing the data
        byte_array = self.data
        start_index = recordObj.getStart()                                              # get the start index of data inside bytearray
        last_index = recordObj.getEnd()                                                 # get the end index of data inside bytearray
        for i in range(start_index, last_index):                                        # for loop to read byte_array
            returnData.append(byte_array[i])                                            # full returnData list
        return returnData                                                               # return the returnData list
    
    def read_byte_by_index(self, recordObj, column):                        # read data inside page bytearray() for a specified column
        bytearr = self.data
        returnData = []
        start_index = recordObj.getStart()                                              # get the start index of data inside bytearray
        last_index = recordObj.getEnd()                                                 # get the end index of data inside bytearray
        returnval = bytearr[start_index + column] 
        for i in range(start_index, last_index):                                        # for loop to read byte_array
            returnData.append(bytearr[i])                                            # full returnData list
        return returnval                                                               # return the returnData list

    def recordColDel(self, RecordObj):                                                  # delete column data from record object after writing to byte array
        RecordObj.columns = None
        return RecordObj
    
    def space_allocation(self, byte_allocation:int):

        self.num_records += 1
        # Find the start and end indexes
        start_index = self.nextDataBlock
        self.nextDataBlock += byte_allocation

        return start_index, self.nextDataBlock

    def store_hex_in_bytearray(self, hex_value:hex):
        # Convert hex string to bytes
        hex_bytes = bytes.fromhex(hex_value)

        # Create a bytearray of the specified size
        result = self.data

        # get start position for writing 
        start = self.nextDataBlock

        # Calculate how much of the bytearray will be filled
        bytes_to_fill = min(PAGE_SIZE, len(hex_bytes))

        bytes_to_fill = start+bytes_to_fill
        # Fill the beginning of the bytearray with the hex bytes
        result[start:bytes_to_fill] = hex_bytes[:bytes_to_fill]

        self.num_records += 1
        self.nextDataBlock = bytes_to_fill

        return start, self.nextDataBlock
    
    def store_hex_in_bytearray_by_index(self, hex_value:hex, start, end):
        # Convert hex string to bytes
        hex_bytes = bytes.fromhex(hex_value)

        # Create a bytearray of the specified size
        result = self.data

        # get start position for writing 
        start = start

        bytes_to_fill = end
        # Fill the beginning of the bytearray with the hex bytes
        result[start:bytes_to_fill] = hex_bytes[:bytes_to_fill]

        return result[start:bytes_to_fill]

    def parse_integer_to_nibbles(self, number:int):

        if not isinstance(number, int) or number < 0:
            raise ValueError("Input must be a non-negative integer")

        # Convert the number to a string to iterate over its digits
        number_str = str(number)

        # get start position for writing 
        start = int(self.nextDataBlock)

        # Calculate the number of nibbles needed to store all digits
        num_nibbles = (len(number_str) + 1) // 2

        # Create a byte array to store the nibbles
        nibble_array = self.data

        # Iterate over the digits in pairs to create the nibbles
        for i in range(start, len(number_str), 2):
            # Convert each pair of digits to a nibble
            if i + 1 < len(number_str):
                nibble = int(number_str[i]) << 4 | int(number_str[i + 1])
            else:
                # If there's only one digit left, store it in the leftmost nibble
                nibble = int(number_str[i]) << 4

            # Store the nibble in the byte array
            nibble_array[i // 2] = nibble

        self.nextDataBlock = int((i/2)+1)
        self.num_records += 1


        return start, self.nextDataBlock
    
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
            RecordObj.pageLocStart, RecordObj.pageLocEnd = self.fill_bytearray(self.data, RecordObj.columns, self.nextDataBlock)    # fill the byte array with data and return the (first element location) and (last element location +1)
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