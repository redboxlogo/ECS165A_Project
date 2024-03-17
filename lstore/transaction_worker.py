from threading import Thread
from lstore.locks import ReadWriteLock


class TransactionWorker:
    # Initializes the TransactionWorker with an optional list of transactions.
    def __init__(self, transactions=None):
        # Stores the outcome of each transaction (True for commit, False for abort).
        self.stats = []
        # A list of transactions to be processed. If not provided, defaults to an empty list.
        self.transactions = transactions if transactions is not None else []
        # Tracks the number of transactions that have been successfully committed.
        self.result = 0
        # Manages ReadWriteLock instances for each RID to handle concurrent access.
        self.locks = {}

    # Adds a transaction to the list of transactions to be processed.
    def add_transaction(self, transaction):
        self.transactions.append(transaction)

    # Starts the transaction processing in separate threads for concurrent execution.
    def run(self):
        threads = []  # List to hold the threads for each transaction.
        for transaction in self.transactions:
            # Create a new thread for each transaction, passing the transaction to __run method.
            thread = Thread(target=self.__run, args=(transaction,))
            threads.append(thread)  # Add the new thread to the list.
            thread.start()  # Start executing the thread.

        # Wait for all transaction threads to complete.
        for thread in threads:
            thread.join()

        # Calculate the final result as the sum of successfully committed transactions.
        self.result = sum(self.stats)

    # Method to process each transaction within its thread.
    def __run(self, transaction):
        all_locks_acquired = True
        # Try to acquire the necessary locks for the transaction.
        for lock_type, rid, operation in transaction.operations:
            # Default to creating a new ReadWriteLock for the RID if not already present.
            lock = self.locks.setdefault(rid, ReadWriteLock())
            # Acquire the appropriate lock based on the operation type.
            if lock_type == 'read':
                acquired = lock.get_shared_lock()
            elif lock_type == 'write':
                acquired = lock.get_exclusive_lock()
            else:
                acquired = False  # Handle unexpected lock types gracefully.
            
            # If any lock couldn't be acquired, abort the transaction.
            if not acquired:
                transaction.abort()
                all_locks_acquired = False
                break  # Exit the loop early since the transaction must be aborted.

        # If all locks were successfully acquired, attempt to execute the transaction.
        if all_locks_acquired:
            try:
                # Execute each operation in the transaction.
                for _, _, operation in transaction.operations:
                    operation() 
                # If all operations succeed, commit the transaction.
                transaction.commit()
                self.stats.append(True)  # Mark this transaction as successfully committed.
            except Exception as e:
                # Handle any exceptions that occur during transaction execution.
                print(f"Transaction failed: {e}")
                transaction.abort()
                self.stats.append(False)  # Mark this transaction as aborted.
            finally:
                # Always release the locks acquired during this transaction.
                self.release_locks(transaction)

    # Releases the locks acquired during the transaction.
    def release_locks(self, transaction):
        for lock_type, rid, _ in transaction.operations:
            lock = self.locks.get(rid)  # Retrieve the lock instance for the RID.
            if lock:
                # Release the lock based on the operation type.
                if lock_type == 'read':
                    lock.release_shared_lock()
                elif lock_type == 'write':
                    lock.release_exclusive_lock()
