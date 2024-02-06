
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
        self.record_list.append(RecordObj)
        # print(bytearray(RecordObj.columns))

        # self.data = self.data.append(RecordObj.columns)
        #print(self.data)
        pass

