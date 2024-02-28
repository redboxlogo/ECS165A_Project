from lstore.table import Table
from lstore.bufferpool import *
import os
import shutil
import json
import pickle
from threading import Lock

class Database():

    def __init__(self):
        self.tables = {}  # stores table data
        self.bufferpool = None
        self.table_directory = {}  # maps table names to table information
        self.root = None  # name of the root node
        self.locks = {}  # Dictionary to store locks for each table
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
            with os.scandir(root_path) as entries:  # iterate over the entries in a directory
                for entry in entries:
                    file_path = f"{root_path}/table_directory.pkl"  # access file path
                    if entry.path == file_path:
                        with open(file_path, "rb") as pkl_file:
                            self.table_directory = pickle.load(pkl_file)
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
                 # self.fill_table()

        else:
            os.mkdir(root_path)
            self.root = root_path

    def close(self):

    stores_table_data()
        
        # Iterate through tables and grab all index values
        index_values = []
        for table in self.tables:
            index_values.extend(table.get_index_values())
        # Write dirty pages back into the disk
        for table in self.tables:
            table.flush_page()
        # Save table directory to disk
        file_path = f"{self.root_name}/table_directory.pkl"
        with open(file_path, "wb") as pkl_file:
            pickle.dump(self.table_directory, pkl_file)
        # Release memory
        self.tables = []
        self.bufferpool = None
        self.root_name = None
        self.table_directory = {}

    def acquire_lock(self, table_name):
        if table_name not in self.locks:
            self.locks[table_name] = Lock()
        self.locks[table_name].acquire()

    def release_lock(self, table_name):
        if table_name in self.locks:
            self.locks[table_name].release()

    def release_locks(self, table):
        table_name = table.name
        self.release_lock(table_name)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # First, check if a table with the same name already exists in the database
        for table in self.tables:  # Loop through the existing tables
            if table.name == name:  # If a table with the given name is found
                # Raise an exception to indicate a table with this name already exists
                raise Exception(f"Table '{name}' already exists.")
        
        table_path = f"{self.root}/{name}"  # write name of path for the table
        if os.path.isdir(table_path):  # if table_path exists in directory
            raise Exception(f"Table name, {name}, already exists.")
        else:
            os.mkdir(table_path)  # make the directory
        
        # If no table with the same name exists, proceed to create a new table
        table = Table(name, num_columns, key_index, table_path, self.bufferpool)  # Create a new Table object
        table.index.create_index(0)  # get index of table (should exist in primary column)
        self.tables[name] = table
        # self.tables.append(table)  # Add the newly created table to the list of tables in the database

        # add to table directory
        self.table_directory[name] = {
            "name": name,
            "table_path_name": table_path,
            "num_columns": num_columns,
            "key": key_index
        }
        
        # Return the newly created table object
        return table

    
    """
    populates a table object with data from disk
    """

    def fill_table(self):
        for table_name in self.table_directory:
            table_path = self.table_directory[table_name].get("table_path_name")
            num_columns = self.table_directory[table_name].get("num_columns")
            table_key = self.table_directory[table_name].get("key")
            placeholder = Table(name=table_name, num_columns=num_columns, key=table_key, path=table_path,
                            bufferpool=self.bufferpool, is_new=False)
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

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # Iterate through the list of tables to find the one with the matching name
        for i, table in enumerate(self.tables):
            if table.name == name:
                del self.tables[i]  # Delete the table from the list if found
                return  # Exit the method after deleting the table
        # If the table with the specified name was not found, raise an error
        raise KeyError(f"Table '{name}' does not exist.")

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        # Iterate through the list of tables to find and return the table with the matching name
        for table in self.tables:
            if table.name == name:
                return table  # Return the table instance if found
        # If no table with the specified name is found, raise an error indicating it does not exist
        raise KeyError(f"Table '{name}' does not exist.")
