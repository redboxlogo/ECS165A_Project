"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
import os 
import pickle

class Index:

    def __init__(self, table):#(self, table,root_path)
        self.table = table  # Storing the table object for later use
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.key_to_base_records = {}
        self.rid_to_tail_records = {} 
        #self.root_path = root_path
        return None

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if column in self.indices:
            column_index = self.indices[column]
            if value in column_index:
                return column_index[value]
        return None

    
    #function to directly access records by their IDs
    

    def remove(self, column, key):
        
        if self.indices[column] is not None:
            if key in self.indices[column]:                
                del self.indices[column][key]
                return True
        return False

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def insert_newrec(self, record):
        key = record.key  # record object has self.key
        # Check if the key exists in the indices dictionary
        '''if key not in self.indices:
            # If the key doesn't exist, create an index for it
            return self.create_index(key)'''
        # Add the record to the index for the specified key
        if self.indices[1] == None:
            self.indices[1] = {}
        self.indices[1][key] = key

        self.key_to_base_records[key] = record
        return True  # Successfully inserted the record

    def insert_tailrec(self, record):
        if(record == False):
            print("record is false")
            return False
        rids1 = record.rid  # record object has self.rid
        # Add the tail record to the index for the specified rid
        self.rid_to_tail_records[rids1] = record
        return True  # Successfully inserted the record
    
    def lookup_tail(self,rids1):
        if rids1 in self.rid_to_tail_records:
            return self.rid_to_tail_records[rids1]
        return False

    def lookup(self, key):
        # Check if the key exists in the indices dictionary
        if key in self.indices[1]:
            # Return the record associated with the key
            return self.key_to_base_records[key]
        return None  # Key not found in the index
    
    def locate_range(self, begin, end, column):
        if column in self.indices:
            column_index = self.indices[column]
            records_within_range = {}
            for value in range(begin, end + 1):
                if value in column_index:
                    records_within_range.append(column_index[value])
            return records_within_range
        return []

    """
    # Create index on specific column
    """

    def create_index(self, column_number):
        if column_number not in self.indices:
            # If no index exists, initialize a new index for the column
            self.indices[column_number] = {}  # You can use any appropriate data structure like a dictionary or a B-Tree
            
            # # Iterate over each record in the table to populate the index
            # for record in self.table.records:  # Accessing table object
            #     # Get the value of the specified column using read_byte_by_index function
            #     value = self.read_byte_by_index(record, column_number)
            #     #self.indices[column_number][record.record_id] = record---------------->

            #     # Check if the value is already in the index
            #     if value in self.indices[column_number]:
            #         # If the value already exists, append the record's position to the list of positions
            #         self.indices[column_number][value].append(record.position)
            #     else:
            #         # If the value does not exist, create a new list with the record's position
            #         self.indices[column_number][value] = [record.position]
                    
            return True
        else:
            # If an index already exists for the column, return False to indicate that index creation failed
            return False

    """
    #  Drop index of specific column
    """

    def drop_index(self, column_number):
        if column_number in self.indices.keys():
            self.indices[column_number] = None
            return True
        return False 




def save_index_to_pickle(index_object, file_path):
    """
    Save index data to a pickle file.

    Parameters:
        index_object (object): The Index object containing the index data.
        file_path (str): The path to the pickle file where the index data will be saved.
    """
    try:
        # Create the directory if it doesn't exist
        dirname = os.path.dirname(file_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        # Open the file in binary write mode
        with open(file_path, "wb") as f:
            # Dump the index data into the file
            pickle.dump(index_object.indices, f)
    except Exception as e:
        print(f"Error saving index data to pickle: {e}")

def load_index_from_pickle(file_path, index_object):
    """
    Load index data from a pickle file.

    Parameters:
        file_path (str): The path to the pickle file containing the index data.
        index_object (object): The Index object where the loaded index data will be assigned.

    Returns:
        bool: True if loading is successful, False otherwise.
    """
    try:
        # Check if the file exists
        if os.path.exists(file_path):
            # Open the file in binary read mode
            with open(file_path, "rb") as f:
                # Load the index data from the file
                indices_data = pickle.load(f)

            # Assign the loaded index data to the indices attribute of the Index object
            index_object.indices = indices_data
            return True
        else:
            print("Index file not found.")
            return False
    except Exception as e:
        print(f"Error loading index data from pickle: {e}")
        return False
