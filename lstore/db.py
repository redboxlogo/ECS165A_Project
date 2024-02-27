from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path):
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
        # Check if a table with the same name already exists in the database
        if name in self.tables:  # Directly check for the table name in the dictionary keys
            # Raise an exception to indicate a table with this name already exists
            raise Exception(f"Table '{name}' already exists.")
        
        # Create a new Table object and add it to the dictionary with the table name as the key
        self.tables[name] = Table(name, num_columns, key_index)
        
        # Return the newly created table object
        return self.tables[name]

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # Check if the table exists, and remove it using its name as the key
        if name in self.tables:
            del self.tables[name]  # Remove the table from the dictionary
        else:
            # If the table with the specified name was not found, raise an error
            raise KeyError(f"Table '{name}' does not exist.")

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        # Return the table using its name as the key if it exists
        if name in self.tables:
            return self.tables[name]
        else:
            # If no table with the specified name is found, raise an error
            raise KeyError(f"Table '{name}' does not exist.")
