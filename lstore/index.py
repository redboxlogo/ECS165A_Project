"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        self.table = table  # Storing the table object for later use
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if column in self.indices and value in self.indices[column]:
            return self.indices[column][value]
        return None

    def remove(self, column, key):
        # Check if the column has an initialized index
        # The indices attribute is expected to be a list or dictionary where each entry corresponds to a column's index.
        # Each column's index could be a dictionary itself, mapping record keys to their locations or record IDs (RIDs).
        if self.indices[column] is not None:
            # If the index for the specified column is not None,
            # check if the key to be removed exists in this column's index.
            if key in self.indices[column]:
                # If the key exists, delete it from the column's index.
                # This effectively removes the mapping from the key to its record location or RID,
                # which is part of maintaining the integrity of the index after a record deletion.
                del self.indices[column][key]
                # Return True to indicate that the key was found and successfully removed.
                return True
        # If the column does not have an index initialized or the key does not exist in the column's index,
        # return False. This could indicate that either no records exist for that key
        # or that the column is not indexed.
        return False

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            # If no index exists, initialize a new index for the column
            self.indices[column_number] = {}  # You can use any appropriate data structure like a dictionary or a B-Tree
            
            # Iterate over each record in the table to populate the index
            for record in self.table.records:  # Accessing table object
                # Get the value of the specified column using read_byte_by_index function
                value = self.read_byte_by_index(record, column_number)
                
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
        self.indices[column_number] = None
