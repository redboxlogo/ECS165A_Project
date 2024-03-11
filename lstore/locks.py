from collections import defaultdict
import threading
'''
class rwlock_manager:
    def __init__(self):
        # locks stores a hashtable indicate the lock management for each record 
        self.locks = defaultdict(ReadWriteLock)
        pass 

    def get_shared(self,rid):
        return self.locks[rid].get_shared_lock()

    def release_shared(self,rid):
        return self.locks[rid].release_shared_lock()
    
    def get_exclusive(self,rid):
        return self.locks[rid].get_exclusive_lock()
    
    def release_exclusive(self,rid):
        return self.locks[rid].release_exclusive_lock()
'''

class ReadWriteLock:
    """ A lock object that allows many simultaneous "read locks", but only one exclusive "write lock." 
        * The no-wait policy avoid deadlock 
        TODO: avoid writer starvation 
        TODO: implement wait policy but do deadlock prevention 
    """

    def __init__(self):
        # Avoid race condition on reader and writer counter 
        self.ready_lock = threading.Lock()
        # counts the number of readers who are currently in the read-write lock (initially zero)
        self.readers = 0
        self.writers = False



    def get_shared_lock(self):
        self.ready_lock.acquire()
        if self.writers==True:
            self.ready_lock.release()
            return False
        else:
            self.readers += 1
            self.ready_lock.release()
            return True

    def get_exclusive_lock(self):
        self.ready_lock.acquire()
        if self.readers != 0 :
            self.ready_lock.release()
            return False
        elif self.writers: 
            self.ready_lock.release()
            return False
        else:   
            self.writers = True
            self.ready_lock.release()
            return True

    def release_shared_lock(self):
        self.ready_lock.acquire()
        self.readers -= 1
        self.ready_lock.release()

    def release_exclusive_lock(self):
        self.ready_lock.acquire()
        self.writers = False
        self.ready_lock.release()
