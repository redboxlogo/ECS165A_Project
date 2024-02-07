
class Page:

    def __init__(self, directoryID):
        #self.PageType = PageType                #True = tail page; False = base page
        self.directoryID = directoryID          #page identifier: positive for tail pages; negative for base
        self.num_records = 0
        self.data = bytearray(4096)
        self.nextDataBlock = 0

    def has_capacity(self):
        pass

    def fill_bytearray(self, byte_array, value_list, startloc):

        start_index = startloc
        last_index = min(start_index + len(value_list), len(byte_array))
        for i in range(start_index, last_index):
            byte_array[i] = value_list[i - start_index]  
        return start_index, last_index

    def write(self, RecordObj):
        if(self.nextDataBlock == 4096):
            return False
        self.num_records += 1
        RecordObj.pageLocStart, RecordObj.pageLocEnd = self.fill_bytearray(self.data, RecordObj.columns, self.nextDataBlock)
        self.nextDataBlock = RecordObj.pageLocEnd
        return True


    # # Create a bytearray with 4096 elements initialized to 0
    # my_bytearray = bytearray(4096)

    # # Change the values of elements in the bytearray
    # # For example, let's change the first 10 elements to a different value (e.g., 255)
    # for i in range(20):
    #     my_bytearray[i] = 8

    # # Print the updated bytearray
    # print(my_bytearray)