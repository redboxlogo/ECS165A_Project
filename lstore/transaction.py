from lstore.table import Table, Record
from lstore.index import Index
from lstore.locks import ReadWriteLock
from lstore.query import Query
import datetime

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.name = None  # name of query function
        self.fun = None  # query function
        self.timestamp = datetime.now()  # timestamp
        self.key = None  # key
        self.column = None  # column
        self.columns = None
        self.start_loc = None
        self.end_loc = None
        self.locks = {}
        self.read_write_lock = ReadWriteLock
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        if self.table is None:
           # query_member = getmembers(query, lambda member: isinstance(member, Query))[0][1]
            '''
            getmembers() returns the member functions present in the module passed as an argument of this method
            '''
            self.table = query_member.table  # dumps the table data for the query object in table
        self.queries.append((query, args))
        # use grades_table for aborting
        return

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        # Roll back changes by restoring the original state of all affected records
        for record_id, original_record in self.original_states.items():
            # Assuming Table class has a method to update records by ID
            Table.update_record_by_id(record_id, original_record)
        print("Transaction aborted and changes rolled back.")
        return False

    
    def commit(self):
        # Commit changes made during the transaction
        try:
            for rid, lock_type in self.locks.items():
                if lock_type == 'read':
                    # Release reader lock for the given record identifier
                    self.read_write_lock.release_shared_lock(rid)
                elif lock_type == 'write':
                    # Release writer lock for the given record identifier
                    self.read_write_lock.release_exclusive_lock(rid)
            print("Transaction committed successfully.")
            return True
        except Exception as e:
            # Handle exceptions during commit
            print(f"Error during commit: {e}")
            return False

        