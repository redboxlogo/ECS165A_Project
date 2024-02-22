from page import Page
from table import *

class Bufferpool():

    def __init__(self):
        self.frames = []  # frame
        self.frame_directory = {}  # directory for frame is set to dictionary
        self.frame_count = 0  # frame count

    def add_frame(self):
        pass

    '''
    this function checks if our bufferpool is full
    returns True if full, returns False if not
    '''
    def full(self):
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
