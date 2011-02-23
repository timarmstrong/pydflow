from __future__ import with_statement
"""
Implement a pool of worker threads which traverse the flowgraph.

The threads actually execute the function for @func tasks, but
hand off execution to other providers for other types of tasks

There is a global queue through which other threads request that, 
at some point, a task should be executed. 

Each thread has a deque of tasks to execute which it uses as a stack. 
Threads steal work from each other's deques. 

TODO:
* Smarter thread pool - configurable/adaptive # of workers
* Convert as much code as possible to C
"""
import threading
import Queue
import logging
from collections import deque 
from PyDFlow.futures import Future
from PyDFlow.base.states import *
from PyDFlow.base.flowgraph import graph_mutex
 
import random
NUM_THREADS = 8

# Maximum time to wait before trying to steal work
QUEUE_TIMEOUT = 0.05

# Work first arrives in this queue from other threads
in_queue = None
workers = []
# These deques store the work remaining to be done
work_deques = None

# Use these variables to gracefully let threads go idle
# without them missing out on workstealing opportunities
# The number of workers which were unsuccessful in finding work
# recently
idle_worker_count = 0
idle_worker_cvar = threading.Condition() 

def init():
    global in_queue, workers, work_deques
    logging.debug("Initialising thread pool")
    in_queue = Queue.Queue()
    #TODO: this may not be a good memory layout, likely
    # to cause cache conflict
    work_deques = [deque() for i in range(NUM_THREADS)]
    for i in range(NUM_THREADS):
        t = WorkerThread(in_queue, i, work_deques[i])
        workers.append(t)
        t.start()

# Use future to ensure init() run exactly once
initFuture = Future(function=init)

def force_async(task):
    """
    Takes an object callable with no arguments, and executes it
    at some point in the future
    """
    # Ensure workers are initialized
    logging.debug("Entered force_async")
    initFuture.get()
    logging.debug("Added task to work queue")
    in_queue.put(task)
    with idle_worker_cvar:
        idle_worker_cvar.notifyAll()

ReturnMarker = object()

PYFUN_THREAD_NAME = "pyfun_thread"

def makeframe(task):
    """
    Make a task frame from a task.
    Graph mutex should be held.
    """
    return (task, [ch for ch in task._inputs if ch._try_readable()])

class WorkerThread(threading.Thread):
    """ 
    Worker thread class: repeatedly grabs callable items
    from work_queue, and runs them
    """
    def __init__(self, queue, worker_num, work_deque):
        threading.Thread.__init__(self, name=("%s_%d" % (PYFUN_THREAD_NAME, worker_num)))
        self.queue = queue
        
        # TODO: change logic to expect lists of tasks in deque
        self.deque = work_deque
        self.worker_num = worker_num
        self.setDaemon(True) # Ensure threads will exit with application
        self.last_steal = worker_num
        self.idle = False
        self.continuation = None
    
    def run(self, recursive=False):
        logging.debug("Thread %s starting up" % self.name)
        while True:
            logging.debug("Thread %s\n  Deque: %s" % (self.name, repr(self.deque)))
            if self.continuation is not None:
                self.run_continuation()
                continue
            # Try to run own work
            if self.run_from_deque():
                continue
            
            if recursive:
                # If this was called recursively, we should return once we see
                # the end of the deque.  This will only happen if all of the recursive
                # work has either been finished or stolen 
                return
            
            # Try to get work from global queue (until timeout) 
            if self.run_from_queue():
                continue 
            
            # Try to steal work
            if self.steal_work():
                continue
            
            # If there was no work, see if we can establish consensus 
            # among threads that there is no work to do
            taskframe = self.try_idle()
            if taskframe is not None:
                self.eval_taskframe(taskframe)
                
    def run_continuation(self):
        cont = self.continuation
        self.continuation = None
        logging.debug("%s: running a sequence of tasks as continuation %s" %
                      (self.name(), repr(cont)))
        # Run tasks in order of dependency
        for task in reversed(cont):
            assert(task._state == T_DATA_READY)
            task.set_state(T_QUEUED)
            self.exec_task(task)
 
            
    def exec_task(self, task, contstack):
        """
        Start the task running
        """
        #TODO: dispatch to executor?
        logging.debug("%s starting task" % self.name())
        synchronous = True
        if synchronous:
            task._exec(DoneContinuation(contstack, self))
        else:
            task._exec(DoneContinuation(contstack))
        logging.debug("%s passed off task to execute" % self.name())
            
    def run_from_deque(self):
        try:
            taskframe = self.deque.pop()
            if taskframe is ReturnMarker:
                return False
            self.eval_taskframe(taskframe)
            return True
        except IndexError:
            logging.debug("Thread %s - nothing in Deque" % (self.name))
            return False
        
    def run_from_queue(self):
        """
        Returns True if found, false otherwise
        """
        try:
            task = self.queue.get(timeout=(QUEUE_TIMEOUT * random.random()))
            self.queue.task_done()
            with graph_mutex:
                # Build the taskframe: the task and all of its unresolved
                # dependencies
                taskframe = makeframe(task)
            self.eval_taskframe(taskframe)
        except Queue.Empty:
            logging.debug("Thread %s queue timeout" % (self.name))
            return None
        
    def try_idle(self):
        """
        Try to see if work is exhausted and threads can go idle.
        The threads keep on polling until there is a consensus
        that there is no work.
        """
        global idle_worker_count, idle_worker_cvar
        with idle_worker_cvar:
            if not self.idle:
                self.idle = True
                idle_worker_count += 1
            while idle_worker_count == NUM_THREADS:
                # all threads idle
                idle_worker_cvar.wait()
                try:
                    taskframe = self.queue.get(timeout=(QUEUE_TIMEOUT * random.random()))
                    # wake up all
                    idle_worker_count = 0
                    idle_worker_cvar.notifyAll()
                    return taskframe
                except Queue.Empty:
                    pass
        self.idle = False
    
    def steal_work(self):
        """
        Attempts to steal work using a round robin scheme.
        Returns True if succeeds in stealing one task
        returns False if unsuccessful
        """
        victim1 = -1
        victim = (self.last_steal + 1) % NUM_THREADS
        # try each of the threads in turn in a round robin fashion
        # TODO: randomise somewhat so we don't have bus pileup effect
        while victim != victim1:
            try:
                taskframe = work_deques[victim].popleft()
                while taskframe is not ReturnMarker:
                    taskframe = work_deques[victim].popleft()
                # Run and then go back to normal loop
                #print ("Thread %s stole task %s" % (self.name, repr(taskframe[0])))
                logging.debug("Thread %s stole task %s" % (self.name, repr(taskframe[0])))
                self.eval_taskframe(taskframe)
                return True
            except IndexError:
                pass
            # keep track of the first victim
            if victim1 < 0:
                victim1 = victim
            victim = (victim + 1) % NUM_THREADS
        logging.debug("Thread %s couldn't find task to steal" % (self.name))
        return False
            
    def eval_taskframe(self, taskframe):
        """
        taskframe is: (task, 
                [channel dependencies which may not have been picked by an evaluator])
        """        
        task = taskframe[0]
        # If ready to run, this thread runs the task
        #TODO: locking, I don't think we can assume exclusive
        # access
        
        # Check whether any tasks were added to the stack:
        # should only add one to permit workstealing
        logging.debug("%s: looking at task frame %s " % (self.name, repr(taskframe)))
        state = task.state()
        if state in [T_DATA_READY, T_QUEUED]:
            #  All dependencies satisfied
            if state == T_DATA_READY:
                task.set_state(T_QUEUED)
            self.exec_task(task, None)
        elif state in [T_DONE_SUCCESS, T_RUNNING]:
            # already started elsewhere
            return
        elif state == T_DATA_WAIT:
            task, contstack = self.find_runnable_task(taskframe)
            self.exec_task(task, contstack)
        elif state == T_ERROR:
            #TODO: propagate properly
            raise Exception("Running task resulted in error")
        elif state == T_INACTIVE:             
            raise Exception("Tried to execute task %s with inactive state, should not be possible" % repr(task))
        else:
            raise Exception("Task %s has invalid state %d" % ((repr(task), state)))
    
    def force_recursive(self, channel):
        """
        Assume we are holding the global mutex when this is called
        """
        # TODO: lock
        
        # the task to run
        to_run = []
        for task in channel._in_tasks:
            state = task._state
            if state in [T_DATA_READY, T_DATA_WAIT, T_INACTIVE]:
                to_run.append(makeframe(task))
            elif state in [T_QUEUED, T_RUNNING, T_DONE_SUCCESS]:
                pass
            elif state == T_ERROR:
                # exception needs to percolate up to next task running 
                raise Exception("%s encountered an error in recursive invocation" % 
                                                                        repr(task))
            else:
                raise Exception("%s had invalid state" % repr(task))
                
        if len(to_run) == 0:
            # No work to do, return
            return
        else: 
            # add marker to deque to ensure we don't
            # start executing old tasks
            self.deque.append(ReturnMarker)
            self.deque.extend(to_run)
            graph_mutex.release()
            try:
                self.run(recursive=True)
            finally:
                graph_mutex.acquire()
    
    def resume_continuation(self, continuation):
        if self.continuation is not None:
            raise Exception("Two continuations received :(")
        self.continuation = continuation
    
    def find_runnable_task(self, taskframe):
        """        
        This does a depth first search until it finds a runnable task and leaves unexplored
        branches in frames on the deque.
        
        It gathers all of the tasks which have only a single dependency.
        
        Returns a runnable task
        """
        next_taskframe = taskframe
        
        # If we have a bunch of tasks depending only on each other ina  row,
        # add them into this stack
        continuation_stack = [taskframe]
        with graph_mutex:
            while 1:
                dep_count = 0 # number of tasks that need to finish before this task can run
                taskframe = next_taskframe
                next_taskframe = None
                # add unrunnable task to stack to visit later
                self.deque.append(taskframe)
                # Looks at inputs that might not yet be ready
                for i, ch in enumerate(taskframe[1]):
                    # TODO: this is assuming atomic channel
                    if ch is None:
                        continue
                    elif ch._readable():
                        # is ready.. don't do anything
                        taskframe[1][i] = None
                        continue
                    # Next task to look at
                    for in_t in ch._in_tasks:
                        state = in_t._state
                        if state == T_INACTIVE:
                            # do dfs on this task to run dependencies
                            in_t._state = T_DATA_WAIT
                            
                            # We will recurse on this one
                            if next_taskframe is None:
                                taskframe[1][i] = None
                                next_taskframe = makeframe(in_t)
                            dep_count += 1
                        elif state == T_DATA_READY:
                            # run this task
                            in_t._state = T_QUEUED
                            # keep state, data_wait should change to queued when
                            # enabled
                            logging.debug("selected %s to run for thread %s" % 
                                          (repr(in_t), self.name))
                            
                            # record that this is being taken care of
                            taskframe[1][i] = None
                            return (continuation_stack, in_t) # TODO pipeline
                        elif state == T_DONE_SUCCESS:
                            pass
                        elif state in [T_DATA_WAIT, T_RUNNING, T_QUEUED]:
                            # Will be filled with data at some point by another executor
                            dep_count += 1
                        else:
                            raise Exception("Invalid task state for %s" %
                                            (repr(taskframe)))
                if dep_count == 0:
                    raise Exception("Invalid task frame state %s, all inputs \
                                    were ready but state said otherwise " %
                                            repr(taskframe))
                elif dep_count == 1:
                    continuation_stack.append(next_taskframe)
                else:
                    continuation_stack = []
                    
def isWorkerThread():
    return threading.current_thread().name[:len(PYFUN_THREAD_NAME)] == PYFUN_THREAD_NAME


class DoneContinuation:
    def __init__(self, contstack, thread=None):
        if self.contstack is None or len(self.contstack) == 0:
            self.contstack = None
        else:
            self.contstack = contstack
        
        # If thread is provided, we will hand the continuation directly to
        # the thread.  This makes most sense when the task was synchronous
        # as we will not do anything funny with the thread
        # Otherwise we will put the continuation into a global resume queue 
        self.thread = thread
        
    def __call__(self, task, return_val):
        """
        FOr now, assume mutex is held by caller
        """
        if return_val is None:
            task._state = T_ERROR
            #TODO: exception type
            raise Exception("Got None return value")
        
        # Fill in all the output channels
        if len(task._outputs) == 1:
            # Update current state, then pass data to channels
            task._state = T_DONE_SUCCESS
            task._outputs[0]._set(return_val)
        else:
            try:  
                return_vals = tuple(return_val)
            except TypeError:
                #TODO: exception types
                task._state = T_ERROR
                raise Exception("Expected tuple or list of length %d as output, but got something not iterable" % (len(task._outputs)))
            if len(return_vals) != len(task._outputs):
                task._state = T_ERROR
                raise Exception("Expected tuple or list of length %d as output, but got something of length" % (len(task._outputs), len(return_vals)))
    
            # Update current state, then pass data to channels
            task._state = T_DONE_SUCCESS
            for val, chan in zip(return_vals, task._outputs) :
                chan._set(val)
        if self.contstack is None:
            if self.thread is not None:
                #send to specific thread
                self.thread.resume_continuation(self.contstack)
            else:
                resume_continuation(self.contstack)

def resume_continuation(cont):
    """
    TODO: add to queue
    """
    pass