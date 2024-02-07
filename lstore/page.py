
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

        pass

        # # Create a bytearray with 4096 elements initialized to 0
        # my_bytearray = bytearray(4096)

        # # Change the values of elements in the bytearray
        # # For example, let's change the first 10 elements to a different value (e.g., 255)
        # for i in range(20):
        #     my_bytearray[i] = 8

        # # Print the updated bytearray
        # print(my_bytearray)