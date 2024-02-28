from lstore.page import *
from lstore.table import *

BUFFERPOOL_FRAME_COUNT = 100


class Bufferpool():

    def __init__(self, path2root):
        self.frames = []  # frame
        self.frame_directory = {}  # directory for frame is set to dictionary
        self.frame_count = 0  # frame count
        self.path2root = path2root

    '''
    passes frames to frame directory
    '''
    def frame_to_dict(self, table_name, page_range, bpage, brecord, frame_index):  # maybe add in page range?
        frame_key = (table_name, page_range, bpage, brecord)
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
        LUP = self.frames[0]  # last used page as first page in list of frames
        index = 0  # index of page
        frame_index = 0  # index of frame
        min_count = LUP.access_count  # initialize minimum count to how often the last used page was accessed

        for frame in self.frames:
            if frame.access_count < min_count:
                # if the access count of the current frame is less than the minimum count
                # we set the last used page to the current frame
                min_count = frame.access_count
                LUP = frame
                frame_index = index
            elif frame.access_count == min_count:
                if frame.time_in_bufferpool < LUP.time_in_bufferpool:
                    # if the access count is the same as the minimum count
                    # and if the frame has spent less time in the bufferpool than LUP
                    # update the minimum access count to the access count of the current frame
                    min_count = frame.access_count
                    LUP = frame  # update the last used page to the current frame
                    frame_index = index  # update the index of the frame
            index += 1

        if LUP.dirty:  # if the page is marked as dirty
            path = LUP.disk_path2page  # get the path of LUP
            all_cols = LUP.cols  # get the column data
            write_to_disk(path, all_cols)  # write the page to disk

        frame_key = LUP.tuple_key  # update the frame key
        del self.frame_directory[frame_key]  # release memory associated with the frame key

        return frame_index  # returns index of frame that was evicted

    def commit_page(self, index):  # passes in index of frame, commits the page to disk
        current_frame = self.frames[index]  # identify frame we wish to commit
        all_cols = current_frame.cols  # get all column data
        path2page = current_frame.disk_path2page  # get the path to page on disk for the frame
        if current_frame.dirty:  # if the frame is dirty
            write_to_disk(path2page, all_cols)  # write them to disk

    def commit_frames(self):  # commits all frames when closing the database
        for index in range(len(self.frames)):  # iterate through all frames
            if self.frames[index].dirty:  # if frame is dirty
                self.commit_page(index)  # commit it to disk

class Frame():

    def __init__(self, table_name, disk_path2page):
    def __init__(self, table_name, disk_path2page):
        self.cols = []  # all column data
        self.dirty = False  # indicates dirty page
        self.dirty = False  # indicates dirty page
        self.pinned = False  # indicates whether a frame is pinned or unpinned
        self.time = 0  # records time in bufferpool
        self.access_count = 0  # number of times page has been accessed
        self.key = None  # holds a tuple key that uniquely identifies the frame; temporary
        self.disk_path2page = disk_path2page  # path to page on disk
        self.table_name = table_name  # name of table associated with frame

    '''
    setter function for dirty page
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
        return True
        
    '''
    indicates the frame has been unpinned
    '''
    def unpin(self):
        self.pinned = False
        return True
