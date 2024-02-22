from page import Page


class Frame():

    def __init__(self):
        self.page = Page()  # initialize page from Page() class
        self.cols = []  # all columns
        self.dirty = False  # indicates dirty bit
        self.pinned = False  # indicates whether a frame is pinned or unpinned
        self.time = 0  # records time in bufferpool
