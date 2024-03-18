from threading import Thread
from lstore.table import Table, Record
from lstore.index import Index
from lstore.locks import ReadWriteLock


class TransactionWorker:
    """
    Creates a transaction worker object.
    """

    def __init__(self, transactions=None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.thread = None  # Thread instance will be stored here
        self.locks = {}

    """
    Appends t to transactions.
    """

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transactions as a thread.
    """

    def run(self):
        # __run is a private method and should not be passed directly to Thread;
        # Instead, we should define a public method to be called by the thread.
        # We'll define a public method 'execute_transactions' that calls '__run'.
        self.thread = Thread(target=self.execute_transactions, daemon=True)
        self.thread.start()

    """
    Waits for the worker to finish.
    """

    def join(self):
        self.thread.join()

    """
    The public method that the thread will call.
    """
    def execute_transactions(self):
        self.__run()

    """
    Private method to run the transactions.
    """
    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
            # stores the number of transactions that committed
        self.result = sum(1 for result in self.stats if result)
        
