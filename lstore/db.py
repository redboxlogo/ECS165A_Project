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
