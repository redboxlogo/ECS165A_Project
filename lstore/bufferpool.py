from lstore.page import Page
from lstore.table import *

BUFFERPOOL_FRAME_COUNT = 100


class Bufferpool():

    def __init__(self):
        self.frames = []  # frame
        self.frame_directory = {}  # directory for frame is set to dictionary
        self.frame_count = 0  # frame count

    '''
    passes frames to frame directory
    '''
    def frame_to_dict(self, table_name, bpage, brecord, frame_index):  # maybe add in page range?
        frame_key = (table_name, bpage, brecord)
        self.frame_directory[frame_key] = frame_index 
        self.frames[frame_index].tuple_key = frame_key

        if self.frame_count < BUFFERPOOL_FRAME_COUNT:
            self.frame_count += 1
            # print("Frame added successfully to bufferpool")

    '''
    this function checks if our bufferpool is full
    returns True if full, returns False if not
    '''
    def full(self):
        if self.frame_count == BUFFERPOOL_FRAME_COUNT:
            return True
        else:
            return False

    def fetch_page(self, page_id):
        # Implementation for fetching a page into the bufferpool
        pass

    def evict_page(self):
        # Implementation for evicting a page from the bufferpool
        pass

    def flush_page(self, page_id):
        # Implementation for writing a dirty page back to disk
        pass

class Frame():

    def __init__(self, table_name, disk_path2page):
        self.cols = []  # all column data
        self.dirty = False  # indicates dirty page
        self.pinned = False  # indicates whether a frame is pinned or unpinned
        self.time = 0  # records time in bufferpool
        self.access_count = 0  # number of times page has been accessed
        self.tuple_key = None  # holds a tuple key that uniquely identifies the frame; temporary
        self.disk_path2page = disk_path2page  # path to page on disk
        self.table_name = table_name  # name of table associated with frame

    '''
    setter function for dirty page
    '''
    def set_dirty(self):
        self.dirty = True
        return True

    '''
    unset function for dirty page
    '''
    def unset_dirty(self):
        self.dirty = False
        return True
    
    '''
    indicates the frame is pinned
    '''
    def pin(self):
        self.pinned = True
        return True
        
    '''
    indicates the frame has been unpinned
    '''
    def unpin(self):
        self.pinned = False
        return True
