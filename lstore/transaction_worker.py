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
        self.thread = threading.Thread(target=self.__run, daemon=1)
        self.thread.start()

    """
    Waits for the worker to finish.
    """

    def join(self):
        self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
            # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
