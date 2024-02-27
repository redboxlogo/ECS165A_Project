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

    def fetch_page(self, table_name, file_path, bpage, brecord):
        # Check if the page is already in the buffer pool
        for frame in self.frames:
            if frame.tuple_key == (table_name, bpage, brecord):  # Adjust tuple key comparison
                frame.access_count += 1  # Increment access count since the page is accessed
                return frame

        # If the page is not in the buffer pool, fetch it from disk
        if not self.full():
            # Create a new frame
            new_frame = Frame(table_name, file_path)  # Assuming page_id is a tuple (table_name, file_path, bpage, brecord)
            # Load the page content into the frame
            new_frame.read_from_disk(file_path, bpage)  # Assuming file_path is file_path, bpage is bpage
            # Set the frame as dirty since it's fetched from disk
            new_frame.set_dirty()
            # Pin the frame as it's being used
            new_frame.pin()
            # Add the frame to the buffer pool
            self.frames.append(new_frame)
            self.frame_count += 1
            return new_frame
        else:
            # If the buffer pool is full, evict a page
            evicted_frame_index = self.evict_page()
            # Fetch the page from disk and load it into the evicted frame
            evicted_frame = self.frames[evicted_frame_index]
            evicted_frame.read_from_disk(file_path, bpage)  # Assuming file_path is file_path, bpage is bpage
            # Set the frame as dirty since it's fetched from disk
            evicted_frame.set_dirty()
            # Pin the frame as it's being used
            evicted_frame.pin()
            # Update the frame directory with the new page_id
            self.frame_directory[(table_name, bpage, brecord)] = evicted_frame  # Adjust tuple key
            return evicted_frame


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