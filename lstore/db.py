from lstore.table import Table
from lstore.bufferpool import *
import os
import shutil
import json
import pickle


class Database:

    def __init__(self):
        self.indices = {}
        self.tables = {}  # stores table data
        self.bufferpool = None  # sets bufferpool
        self.table_directory = {}  # maps table names to table information
        self.root = None  # name of the root node
        pass

    def open(self, root_path):
        self.bufferpool = Bufferpool(root_path)
        # print(os.listdir(root_path))
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
            self.root = root_path  # set root name to the path
            for entry in os.scandir(root_path):
                # print(entry.path)
                file_path = f"{root_path}/table_directory.pkl"  # access file path
                # print(file_path)
                if entry.path == file_path:
                    with open(file_path, "rb") as pkl_file:
                        self.table_directory = pickle.load(pkl_file)
            # print("opened successfully")
            self.fill_table()
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

        else:
            os.mkdir(root_path)
            self.root = root_path
            # print("opened successfully")

    '''
    ensures that the database is properly closed, 
    metadata is saved, 
    changes are flushed to disk, 
    and any memories are released
    '''
    def close(self):
        table_file_dir = open(f"{self.root}/table_directory.pkl", "wb")  # get directory to table files
        pickle.dump(self.table_directory, table_file_dir)  # write table directory to table_file_dir in binary
        table_file_dir.close()  # close the file object

        for table_data in self.table_directory.values():  # iterate through all table data in directory
            table_name = table_data.get("name")  # get name of table
            get_table = self.tables[table_name]  # get table info for the name
            get_table.record_lock = None  # initialize locking
            closed = get_table.table_page_dir_to_disk()  # closes the table page directory; returns True if closed, False if not

            if not closed:
                raise Exception(f"Could not close the page directory: {table_name}")  # raise error if cannot be closed

            index_file = open(f"{get_table.table_path}/indices.pkl", "wb")  # save indexes as pkl file

            # for index_dict in get_table.index.indices:
                # if index_dict is not None:
                    # for index in index_dict.values():
                        # index.lock = None  # AttributeError: 'int' object has no attribute 'lock'

            pickle.dump(get_table.index, index_file)  # load in index data from get_table into index_file
            index_file.close()  # close the index file

        self.bufferpool.commit_frames()  # commits frames and writes dirty pages to disk

        return True

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):

        table_path = f"{self.root}/{name}"  # write name of path for the table
        if os.path.isdir(table_path):  # if table_path exists in directory
            raise Exception(f"Table name, {name}, already exists.")

        else:
            os.mkdir(table_path)  # make the directory

        # If no table with the same name exists, proceed to create a new table
        table = Table(name, num_columns, key_index, table_path, self.bufferpool)  # Create a new Table object
        # table.index.create_index(0)
        self.tables[name] = table
        # print(self.tables)
        # self.tables.append(table)  # Add the newly created table to the list of tables in the database

        # add to table directory
        self.table_directory[name] = {
            "name": name,
            "table_path": table_path,
            "num_columns": num_columns,
            "key": key_index
        }
        # print(self.table_directory)
        # print(self.tables)
        return table

    def index_create_table(self, name):
        
        if name not in self.tables:
            # If the table doesn't exist, create a new table
            self.tables[name] = {}  # You can use any appropriate data structure like a dictionary or a list for the table
            
            return True
            
        else:
            # If the table already exists, return False to indicate that table creation failed
            
            return False

    """
    populates a table object with data from disk
    """

    def fill_table(self):
        for table_name in self.table_directory:
            table_path = self.table_directory[table_name].get("table_path")
            num_columns = self.table_directory[table_name].get("num_cols")
            table_key = self.table_directory[table_name].get("key")
            placeholder = Table(table_name, num_columns, table_key, table_path, self.bufferpool)
            path2page_dir = f"{table_path}/page_directory.pkl"
            with open(path2page_dir, "rb") as page_dir:
                placeholder.page_dir = pickle.load(page_dir)
            page_dir.close()

            table_data = placeholder.page_dir["table_data"]
            placeholder.fill_in_table_data(table_data)

            # read index from disk
            indices_path = f"{table_path}/indices.pkl"
            with open(indices_path, "rb") as stored_index:
                placeholder.index = pickle.load(stored_index)
            self.tables[table_name] = placeholder
            # print(self.tables[table_name])

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.table_directory:  # check if table name in directory
            table_dir_path = self.table_directory[name]["table_path"]  # get the table directory path
            # print(table_dir_path)
            # print(os.path.exists(table_dir_path))
            if os.path.isdir(table_dir_path):  # if directory exists
                shutil.rmtree(table_dir_path)  # delete the directory
                del self.table_directory[name]  # delete table name from directory
                del self.tables[name]  # delete table name from table dictionary
                print("dropped successfully")
                return True

        # print("Unable to drop table")
        # print(self.table_directory)
        return False

    """
    # Returns table with the passed name
    """
    
    def get_table(self, name):
        if name in self.tables:
            # If the table exists, return a copy of the table
            table_copy = dict(self.tables[name])  # Creating a shallow copy of the table
            return table_copy
        else:
            # If the table doesn't exist, return None
            return None
