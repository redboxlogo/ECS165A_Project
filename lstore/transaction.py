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
            key = args[0]  # get the key for args
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
        # initiate roll back for all changes
        # restore to consistent state
        for key in self.s_locks:
            self.table.lock_manager[key].release_shared_lock()
        for key in self.x_locks:
            self.table.lock_manager[key].release_exclusive_lock()
        for key in self.insert_locks:
            del self.table.lock_manager[key]
        return False

    def commit(self):
        # commit changes in transaction
        for query, args in self.queries:
            query(*args)
            # remove lock from lock manager directory after deleting a record
            if query == Query.delete:
                del self.table.lock_manager[key]
                if key in self.x_locks:
                    self.insert_locks.remove(key)
                if key in self.insert_locks:
                    self.insert_locks.remove(key)

        for key in self.s_locks:
            self.table.lock_manager[key].release_shared_lock()
        for key in self.x_locks:
            self.table.lock_manager[key].release_exclusive_lock()
        for key in self.insert_locks:
            self.table.lock_manager[key].release_exclusive_lock()
        return True

