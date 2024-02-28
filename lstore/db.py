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
        
        pass

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
        
        # If no table with the same name exists, proceed to create a new table
        table = Table(name, num_columns, key_index)  # Create a new Table object
        self.tables.append(table)  # Add the newly created table to the list of tables in the database
        
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
        # Check if the table exists in the database by looking it up in the self.tables dictionary
        if name in self.tables:
            # If the table exists, delete the table object from the self.tables dictionary
            del self.tables[name]

            # Retrieve the path of the table from the table_directory dictionary
            # The get method is used to safely access the table's path with a default of None if not found
            table_path = self.table_directory.get(name, {}).get("table_path_name", None)
            if table_path and os.path.exists(table_path):
                # If the table path exists on disk, delete the directory and all of its contents
                shutil.rmtree(table_path)

            # Check if the table's entry exists in the table_directory dictionary
            if name in self.table_directory:
                # If it exists, delete the entry from the table_directory dictionary
                del self.table_directory[name]

                # Update the table_directory.pkl file on disk to reflect the deletion of the table
                # This ensures that the persistent state of the database is consistent with its in-memory state
                file_path = os.path.join(self.root, "table_directory.pkl")
                with open(file_path, "wb") as pkl_file:
                    # Dump the updated table_directory dictionary to the file
                    pickle.dump(self.table_directory, pkl_file)
        else:
            # If the table does not exist in the self.tables dictionary, raise a KeyError
            # This exception informs the user or calling function that the operation could not be completed because the table was not found
            raise KeyError(f"Table '{name}' does not exist.")

    """
    # Returns table with the passed name
    """
    
    def get_table(self, name):
        # Check if the table name exists as a key in the self.tables dictionary
        if name in self.tables:
            # If the table exists, return the table object
            return self.tables[name]
        else:
            # If the table does not exist, raise a KeyError with a message indicating the table does not exist
            raise KeyError(f"Table '{name}' does not exist.")
