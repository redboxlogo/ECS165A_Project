from lstore.table import Table, Record
from lstore.index import Index
from lstore.locks import ReadWriteLock 
import threading

class TransactionWorker:
    """
    Creates a transaction worker object.
    """
    def __init__(self, transactions=None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.thread = None  # Thread instance will be stored here
        self.locks = {}  # Manages ReadWriteLock instances for each record ID

    """
    Appends t to transactions.
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transactions as a thread.
    """
    def run(self):
        threads = []
        for transaction in self.transactions:
            thread = threading.Thread(target=self.__run, args=(transaction,))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
            # here you need to create a thread and call __run
    

    """
    Waits for the worker to finish.
    """
    def join(self):
        # Wait for the thread to finish if it has been started.
        if self.thread is not None:
            self.thread.join()

    def __run(self):
        # This method runs in a separate thread and processes each transaction.
        for transaction in self.transactions:
            # Phase 1: Growing Phase (acquire all locks needed).
            all_locks_acquired = True
            for op in transaction.operations:
                rid = op[1]
                lock_type = op[0]
                # Adjusted lock management to use ReadWriteLock instances from self.locks.
                lock = self.locks.setdefault(rid, ReadWriteLock())
                
                if lock_type == 'read':
                    acquired = lock.get_shared_lock()
                elif lock_type == 'write':
                    acquired = lock.get_exclusive_lock()
                
                if not acquired:
                    # Abort the transaction if a lock cannot be acquired.
                    transaction.abort()
                    all_locks_acquired = False
                    break  # No need to try to acquire further locks.

            if not all_locks_acquired:
                # If any lock was not acquired, continue to the next transaction.
                self.stats.append(False)
                continue

            try:
                # Execute the transaction operations here.
                for op in transaction.operations:
                    operation = op[2]  # Assuming the operation callable is the third element.
                    operation()  # Execute operation.

                # Commit the transaction if all operations are successful.
                transaction.commit()
                self.stats.append(True)
            except Exception as e:
                # Log the exception or handle it accordingly.
                print(f"Transaction failed: {e}")
                transaction.abort()
                self.stats.append(False)
            finally:
                # Phase 2: Shrinking Phase (release all locks regardless of commit/abort).
                for op in transaction.operations:
                    rid = op[1]
                    lock_type = op[0]
                    lock = self.locks[rid]
                    if lock_type == 'read':
                        lock.release_shared_lock()
                    elif lock_type == 'write':
                        lock.release_exclusive_lock()

        # At the end of processing all transactions, count the successful commits.
        self.result = sum(self.stats)  # Counting True values indicating committed transactions.
