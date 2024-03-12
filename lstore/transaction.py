from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.lock_manager = None
        self.abort = False
        self.locked_keys # maps primary keys to lock type
        self.locks = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args)::
        if self.table is None:
            query_member = getmembers(query, lambda member: isinstance(member, Query))[0][1]
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
        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        # TODO: commit to database
        return True

