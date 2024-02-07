
class Page:

    def __init__(self, directoryID):
        #self.PageType = PageType                #True = tail page; False = base page
        self.directoryID = directoryID          #page identifier: positive for tail pages; negative for base
        self.num_records = 0
        self.record_list = []                   #temporary until we figure out bytearray()
        self.data = bytearray(4096)

    def has_capacity(self):
        pass

    def write(self, RecordObj):
        self.num_records += 1
        # self.record_list.append(RecordObj)
        # print(bytearray(RecordObj.columns))

        # self.data = self.data.append(RecordObj.columns)
        #print(self.data)

        for i in range(len(RecordObj.columns)):
            self.data[i] = 5#RecordObj.columns[i]
            print(self.data[0:i])


        for i in range(min(len(RecordObj.columns), len(self.data[i]))):
            self.data[i] = RecordObj.columns[i]

        pass

    def fill_bytearray(byte_array, value_list):
        
        start_index = byte_array.find(b'\x00') + 1 if b'\x00' in byte_array else 0

        for i in range(start_index, min(start_index + len(value_list), len(byte_array))):
        
            byte_array[i] = value_list[i - start_index]

    # # Create a bytearray with 4096 elements initialized to 0
    # my_bytearray = bytearray(4096)

    # # Change the values of elements in the bytearray
    # # For example, let's change the first 10 elements to a different value (e.g., 255)
    # for i in range(20):
    #     my_bytearray[i] = 8

    # # Print the updated bytearray
    # print(my_bytearray)