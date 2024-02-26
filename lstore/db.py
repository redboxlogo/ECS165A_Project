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
        table = Table(name, num_columns, key_index)
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
