"""
Implements basic thread pool functionality - nothing special, just has a bunch 
of threads and calls functions passed to the queue

TODO:
* Smarter thread pool - configurable/adaptive # of workers
* Time limits on tasks
* Convert as much code as possible to C
"""
import sys
import threading
import Queue
import traceback
import logging


NUM_THREADS = 8

# Keep track of whether data structures have been initialised
structure_lock = threading.Lock()
work_queue = None
workers = []

def ensure_init():
    global structure_lock, work_queue, workers
    structure_lock.acquire()
    if work_queue is None:
        logging.debug("Initialising thread pool")
        work_queue = Queue.Queue()
        for i in range(NUM_THREADS):
            t = WorkerThread(work_queue)
            workers.append(t)
            t.start()

    structure_lock.release()

def execute_async(task):
    """
    Takes an object callable with no arguments, and executes it
    at some point in the future
    """
    ensure_init()
    logging.debug("Added task to work queue")
    work_queue.put(task)

PYFUN_THREAD_NAME = "pyfun"
class WorkerThread(threading.Thread):
    """ 
    Worker thread class: repeatedly grabs callable items
    from work_queue, and runs them
    """
    def __init__(self, queue):
        threading.Thread.__init__(self, name=PYFUN_THREAD_NAME)
        self.queue = queue
        self.setDaemon(True) # Ensure threads will exit with application

    def run(self):
        while True: #TODO: terminate condition
            task = self.queue.get()
            try: 
                logging.debug("python_executor: %s starting task" % self.getName())
                task()
                logging.debug("python_executor: %s finished task" % self.getName())
            except:
                sys.stderr.write("Worker thread threw an exception during task:")
                traceback.print_exc()
            self.queue.task_done()

def isWorkerThread():
    return threading.current_thread().name == PYFUN_THREAD_NAME
