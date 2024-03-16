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
        self.table = None
        self.s_locks = set()
        self.x_locks = set()
        self.insert_locks = set()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.table == None:
            self.table = table

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            key = args[0]
            if key not in self.table.lock_manager:
                self.insert_locks.add(key)
                self.table.lock_manager[key] = ReadWriteLock()
            if key not in self.x_locks and key not in self.insert_locks:
                if self.table.lock_manager[key].get_exclusive_lock():
                    self.x_locks.add(key)
                else:
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

        
