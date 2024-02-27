from lstore.table import Table
from lstore.bufferpool import *
import os
import shutil
import json
import pickle

class Database():

    def __init__(self):
        self.tables = {}  # stores table data
        self.bufferpool = None
        self.table_directory = {}  # maps table names to table information
        self.root = None  # name of the root node
        pass

    # Not required for milestone1
    def open(self, root_path):
        self.bufferpool = Bufferpool(root_path)
        # check to see if root path exists
        if os.path.isdir(root_path):
             """
             os.path.isdir() is a function provided by the os.path module in Python. 
             It is used to check whether a given path points to an existing directory.

             Function Signature: os.path.isdir(path)
             path: This is the path whose existence as a directory you want to check.
             Return Value:
             If the path exists and is a directory, os.path.isdir() returns True.
             If the path does not exist or is not a directory, os.path.isdir() returns False.
             """
             self.root_name = root_path  # set root name to the path
             with os.scandir(root_path) as entries:
                 for entry in entries:
             """
             os.scandir() is a function provided by the os module in Python, introduced in Python 3.5. 
             It is used for efficiently iterating over the contents of a directory.

            Function Signature: os.scandir(path='.')
            path: This is the path to the directory whose contents you want to iterate over. By default, it's set to '.', representing the current working directory.
            Return Value:
            It returns an iterator of DirEntry objects, each representing an entry in the directory specified by the path parameter.
            DirEntry Object:
            Each DirEntry object contains information about a specific directory entry, including its name and attributes.
             """
                     file_path = f"{root_path}/table_directory.pkl"  # access file path
                         if entry.path == file_path:
                             with open(file_path, "rb") as pkl_file:
                                 self.table_directory = pickle.load(pkl_file)
                    # self.fill_table()
        else:
            os.mkdir(root_path)
            self.root = root_path
        pass

    def close(self):
        
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)  # creates new table object
        self.tables[name] = table  # assigns table to its name
        return table

    """
    fills in table data from directory
    we get the info from open() and sets it to the table name
    """
    def fill_table(self):
       pass
    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        self.tables[name] = None
        return True

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
