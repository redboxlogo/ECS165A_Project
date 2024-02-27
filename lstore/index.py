"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        self.table = table  # Storing the table object for later use
        # One index for each table. All our empty initially.
        self.indices = {}
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
        key = record.key  # Assuming 'key' is a property or method of the Record object
        # Check if the key exists in the indices dictionary
        '''if key not in self.indices:
            # If the key doesn't exist, create an index for it
            return self.create_index(key)'''
        # Add the record to the index for the specified key
        self.indices[key] = record
        return True  # Successfully inserted the record


    def lookup(self, key):
        # Check if the key exists in the indices dictionary
        if key in self.indices:
            # Return the record associated with the key
            return self.indices[key]
        return None  # Key not found in the index
    
    def locate_range(self, begin, end, column):
        if column in self.indices:
            column_index = self.indices[column]
            records_within_range = []
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
            
            # Iterate over each record in the table to populate the index
            for record in self.table.records:  # Accessing table object
                # Get the value of the specified column using read_byte_by_index function
                value = self.read_byte_by_index(record, column_number)
                #self.indices[column_number][record.record_id] = record---------------->

                # Check if the value is already in the index
                if value in self.indices[column_number]:
                    # If the value already exists, append the record's position to the list of positions
                    self.indices[column_number][value].append(record.position)
                else:
                    # If the value does not exist, create a new list with the record's position
                    self.indices[column_number][value] = [record.position]
                    
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
