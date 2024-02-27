from lstore.page import Page
from lstore.table import *

BUFFERPOOL_FRAME_COUNT = 100


class Bufferpool():

    def __init__(self):
        self.frames = []  # frame
        self.frame_directory = {}  # directory for frame is set to dictionary
        self.frame_count = 0  # frame count

    '''
    this function checks if our bufferpool is full
    returns True if full, returns False if not
    '''
    def full(self):
        # Checks if the buffer pool is full by comparing the current number of frames
        # against the maximum allowed frames defined by BUFFERPOOL_FRAME_COUNT.
        # Returns True if the number of frames is greater than or equal to the maximum allowed,
        # indicating the buffer pool is full.
        # Returns False otherwise, indicating there is still space available.
        return len(self.frames) >= BUFFERPOOL_FRAME_COUNT

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

    def __init__(self):
        self.cols = []  # all column data
        self.dirty = False  # indicates dirty bit
        self.pinned = False  # indicates whether a frame is pinned or unpinned
        self.time = 0  # records time in bufferpool

    '''
    setter function for dirty bits
    '''
    def set_dirty(self):
        self.dirty = True
    
    '''
    indicates the frame is pinned
    '''
    def pin(self):
        self.pinned = True
        
    '''
    indicates the frame has been unpinned
    '''
    def unpin(self):
        self.pinned = False
