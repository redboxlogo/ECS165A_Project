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
    


    def store_hex_in_bytearray(self, hex_value:hex):
        # Convert hex string to bytes
        hex_bytes = bytes.fromhex(hex_value)
        print(hex_bytes)

        # # Convert bytes to hexadecimal string using hex() method
        # hex_string = hex_bytes.hex()
        # print(hex_string)

        # Create a bytearray of the specified size
        result = self.data

        # Calculate how much of the bytearray will be filled
        bytes_to_fill = min(PAGE_SIZE, len(hex_bytes))
        print(bytes_to_fill)

        # Fill the beginning of the bytearray with the hex bytes
        result[self.nextDataBlock:bytes_to_fill] = hex_bytes[:bytes_to_fill]

        self.nextDataBlock = bytes_to_fill



        print(bytes_to_fill)

        return result

    def parse_integer_to_nibbles(self, number:int):

        if not isinstance(number, int) or number < 0:
            raise ValueError("Input must be a non-negative integer")

        # Convert the number to a string to iterate over its digits
        number_str = str(number)

        # Calculate the number of nibbles needed to store all digits
        num_nibbles = (len(number_str) + 1) // 2

        # Create a byte array to store the nibbles
        nibble_array = self.data

        # Iterate over the digits in pairs to create the nibbles
        for i in range(0, len(number_str), 2):
            # Convert each pair of digits to a nibble
            if i + 1 < len(number_str):
                nibble = int(number_str[i]) << 4 | int(number_str[i + 1])
            else:
                # If there's only one digit left, shift it to the leftmost nibble
                nibble = int(number_str[i]) << 4

            # Store the nibble in the byte array
            nibble_array[i // 2] = nibble

        self.nextDataBlock = ((i/2)+1)

        return None
    
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
