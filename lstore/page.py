
PAGE_SIZE = 4096

class Page:

    def __init__(self, directoryID):
        self.directoryID = directoryID          # page identifier: positive for tail pages; negative for base
        self.num_records = 0                    # count total records
        self.record_metadata = []                   # list of records
        self.data = bytearray(PAGE_SIZE)        # page size/data
        self.nextDataBlock = 0                  # page index for next empty area of page

    def has_capacity(self):                     # check remain capacity of page
        return PAGE_SIZE-self.nextDataBlock     # return total remaining capacity
        

    def fill_bytearray(self, byte_array, value_list, startloc):                                                                     # function used to fill column data into bytearray()
        start_index = startloc                                                                                                      # get start location for write op (append)
        last_index = min(start_index + len(value_list), len(byte_array))                                                            # get end index for write op
        for i in range(start_index, last_index):                                                                                    # for loop to fill byte_array
            byte_array[i] = value_list[i - start_index]   
        return start_index, last_index                                                                                              # return the start and last index

    def recordColDel(self, RecordObj):                                                                                              # delete column data from record object after writing to byte array
        RecordObj.columns = None
        return RecordObj
    
    def write(self, RecordObj):

        remainCapacity = self.has_capacity()                                                                                        # get remaining capacity of page
        dataAmmount = len(RecordObj.columns)                                                                                        # get length of data that needs to be written

        if(remainCapacity < dataAmmount):                                                                                           # if page doesnt have enough space for data abort and return false
            return False
        else:
            self.num_records += 1                                                                                                   # if remaining capacity is fine continue
            RecordObj.pageLocStart, RecordObj.pageLocEnd = self.fill_bytearray(self.data, RecordObj.columns, self.nextDataBlock)    # fill the byte array with data and return the (first element location) and (last element location +1)
            self.record_metadata.append(self.recordColDel(RecordObj))                                                               # store dataless metadata for record
            self.nextDataBlock = RecordObj.pageLocEnd                                                                               # update write head with new location
            return True