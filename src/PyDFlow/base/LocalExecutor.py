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
from PyDFlow.base.mutex import graph_mutex
 
import random


#==============================================#
# INPUT QUEUE
#==============================================#
# Maximum time to wait before trying to steal work
# QUEUE_TIMEOUT = 0.05
QUEUE_TIMEOUT = 1
# Work first arrives in this queue from other threads
in_queue = None

#==============================================#
# RESUME QUEUE
#==============================================#
resume_queue = None

# ========================================== #
#  WORKER THREADS
# ===========================================#

#NUM_THREADS = 8
NUM_THREADS = 1
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
    global in_queue, resume_queue, workers, work_deques
    logging.debug("Initialising thread pool")
    in_queue = Queue.Queue()
    resume_queue = Queue.Queue()
    #TODO: this may not be a good memory layout, likely
    # to cause cache conflict
    work_deques = [deque() for i in range(NUM_THREADS)]
    for i in range(NUM_THREADS):
        t = WorkerThread(in_queue, resume_queue, i, work_deques[i])
        workers.append(t)
        t.start()

# Use future to ensure init() run exactly once
initFuture = Future(function=init)

def exec_async(task):
    """
    Takes an object callable with no arguments, and executes it
    at some point in the future
    """
    # Ensure workers are initialized
    logging.debug("Entered exec_async")
    initFuture.get()
    logging.debug("Added task to work queue")
    in_queue.put(task)
    with idle_worker_cvar:
        idle_worker_cvar.notifyAll()

ReturnMarker = object()

PYFUN_THREAD_NAME = "pyfun_thread"

def makeframe(task, continuation):
    """
    Make a task frame from a task. Graph mutex should be held.
    """
    return (task, 
            [ch for spec, ch in task._input_iter() 
             if not spec.isRaw() and not ch._try_readable()],
            continuation)

class WorkerThread(threading.Thread):
    """ 
    Worker thread class: repeatedly grabs callable items
    from work_queue, and runs them.
    
    The internal deque is a thread-safe double ended queue designed to facilitate
    work-stealing.  It contains suspended tasks that are candidates for workstealing.
    
    Each internal element has the following structure:
        (task_that_needs_running, dependency_list, continuation_list)
        task_that_needs_running: a Task object which has been suspended due to dependencies
        dependency_list: a list of input channels to the task which need to be filled
                    each entry is replaced by None when finished.
        continuation_list: a sequence of tasks that can be run from last-to-first once
                the main task finishes
    """
    def __init__(self, in_queue, resume_queue, worker_num, work_deque):
        threading.Thread.__init__(self, name=("%s_%d" % (PYFUN_THREAD_NAME, worker_num)))
        self.in_queue = in_queue
        self.resume_queue = resume_queue
        
        # TODO: change logic to expect lists of tasks in deque
        self.deque = work_deque
        self.worker_num = worker_num
        self.setDaemon(True) # Ensure threads will exit with application
        self.last_steal = worker_num
        self.idle = False
    
    def run(self, recursive=False):
        logging.debug("Thread %s starting up" % self.getName())
        while True:
            logging.debug("Thread %s\n  Deque: %s" % (self.getName(), repr(self.deque)))
            
            # Try to run own work
            if self.run_from_deque():
                logging.debug("%s ran from deque" % self.getName())
                continue
            
            if recursive:
                # If this was called recursively, we should return once we see
                # the end of the deque.  This will only happen if all of the recursive
                # work has either been finished or stolen 
                return
            
            logging.debug("%s try to resume" % self.getName())
            # Try to get a frame from the resume queue
            if self.run_from_queue(self.resume_queue, False, None):
                logging.debug("%s ran from resume" % self.getName())
                continue
            
            logging.debug("%s try to run from new work queue" % self.getName())
            # Try to get work from global queue (until timeout) 
            if self.run_from_queue(self.in_queue, True, QUEUE_TIMEOUT * random.random()):
                logging.debug("%s ran from new work queue" % self.getName())
                continue 
            
            logging.debug("%s try to steal work" % self.getName())
            # Try to steal work
            if self.steal_work():
                logging.debug("%s stole work" % self.getName())
                continue
            
            # If there was no work, see if we can establish consensus 
            # among threads that there is no work to do
            logging.debug("%s trying to go idle, found no work" % self.getName())
            taskframe = self.try_idle()
            if taskframe is not None:
                self.eval_taskframe(taskframe)
            
    def exec_task(self, taskframe):
        """
        Start the task running.
        """
        #TODO: dispatch to executor?
        task = taskframe[0]
        assert(all([d is None for d in taskframe[1]]))
        contstack = taskframe[2]
        logging.debug("%s starting task" % self.getName())
                    
        task.set_state(T_QUEUED)
        if task.isSynchronous():
            callback = DoneContinuation()
            task._exec(callback)
            if contstack is not None:
                for nexttask in reversed(contstack):
                    nexttask.set_state(T_QUEUED)
                    nexttask._exec(callback)
        else:
            task._exec(DoneContinuation(contstack))
            
    def run_from_deque(self):
        try:
            taskframe = self.deque.pop()
            if taskframe is ReturnMarker:
                return False
            self.eval_taskframe(taskframe)
            return True
        except IndexError:
            logging.debug("Thread %s - nothing in Deque" % (self.getName()))
            return False
        
    
    def run_from_queue(self, queue, block, timeout):
        """
        Trys to get some new work from the queue provided
        Returns True if found, false otherwise
        """
        try:
            task = queue.get(block, timeout)
            
            queue.task_done()
            with graph_mutex:
                # Build the taskframe: the task and all of its unresolved
                # dependencies
                taskframe = makeframe(task, [])
            logging.debug("Got %s from queue" % repr(taskframe))
            self.eval_taskframe(taskframe)
            return True
        except Queue.Empty:
            logging.debug("Thread %s queue timeout" % (self.getName()))
            return False
        
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
                # with idle_worker_cvar, try to get
                # something from queue, if there is
                # genuinely nothing there, block on
                # condition
                try:
                    
                    task = self.in_queue.get(False)
                    # got a task: wake up all
                    self.idle = False
                    idle_worker_count = 0
                    idle_worker_cvar.notifyAll()
                    return makeframe(task, [])
                except Queue.Empty:
                    pass
                
                idle_worker_cvar.wait()
        self.idle = False
        return None
    
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
                #print ("Thread %s stole task %s" % (self.getName(), repr(taskframe[0])))
                logging.debug("Thread %s stole task %s" % (self.getName(), repr(taskframe[0])))
                self.eval_taskframe(taskframe)
                return True
            except IndexError:
                pass
            # keep track of the first victim
            if victim1 < 0:
                victim1 = victim
            victim = (victim + 1) % NUM_THREADS
        logging.debug("Thread %s couldn't find task to steal" % (self.getName()))
        return False
            
    def eval_taskframe(self, taskframe):
        """
        taskframe is: (task, 
                [channel dependencies which may not have been picked by an evaluator])
         If ready to run, this thread runs the task
         If not, it searches the graph until it finds a runnable task, adding branches to the deque. 
        """        
    
        #TODO: locking, I don't think we can assume exclusive access
        logging.debug("%s: looking at task frame %s " % (self.getName(), repr(taskframe)))
        task = taskframe[0]
        state = task._state
        if state in (T_DATA_READY, T_QUEUED):
            #  All dependencies satisfied
            self.exec_task(taskframe)
        elif state in (T_DONE_SUCCESS, T_RUNNING):
            # already started elsewhere
            return
        elif state == T_DATA_WAIT:
            runnable_frame = self.find_runnable_task(taskframe)
            
            if runnable_frame is not None:
                self.exec_task(runnable_frame)
                
        elif state == T_ERROR:
            #TODO: propagate properly
            raise Exception("Running task resulted in error")
        elif state == T_INACTIVE:             
            raise Exception("Tried to execute task %s with inactive state, should not be possible" % repr(task))
        else:
            raise Exception("Task %s has invalid state %d" % ((repr(task), state)))
    
    
    
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
        logging.debug("%s: find_runnable_task" % (self.getName()))
        found_runnable = False
        first_iter = True
        with graph_mutex:
            while not found_runnable:
                dep_count = 0 # number of tasks that need to finish before this task can run
                task, deps, continuation = taskframe
                next_task = None
                
                # Looks at inputs that might not yet be ready
                for i, ch in enumerate(deps):
                    # TODO: this is assuming atomic channel
                    # don't need to check if we just created frame while we were holding lock
                    if ch is None:  
                        continue
                    elif first_iter and ch._readable():                          
                        # is ready.. don't do anything
                        logging.debug("%s became readable" % repr(ch))
                        deps[i] = None
                        continue
                    # Next task to look at
                    for in_t in ch._in_tasks:
                        state = in_t._state
                        #logging.debug("%s, %s" % (repr(ch), repr(in_t)))
                        if state == T_INACTIVE:
                            # We will recurse on this one
                            if next_task is None:
                                # do dfs on this task to run dependencies
                                in_t._state = T_DATA_WAIT
                                next_task = in_t
                            dep_count += 1
                        elif state == T_DATA_READY:
                            # This task could be run now
                            if next_task is None:
                                in_t._state = T_QUEUED
                                logging.debug("selected %s to run for thread %s" % 
                                          (repr(in_t), self.getName()))
                                next_task = in_t
                                found_runnable = True
                            dep_count += 1
                        elif state == T_DONE_SUCCESS:
                            pass
                        elif state in (T_DATA_WAIT, T_RUNNING, T_QUEUED):
                            # Will be filled with data at some point by another executor
                            # but we do need to wait for it to finish
                            dep_count += 1
                        else:
                            raise Exception("Invalid task state for %s" %
                                            (repr(taskframe)))
                
                logging.debug("Depends on %d more tasks, next task is %s" % (dep_count, next_task))
                if dep_count == 0:
                    raise Exception("Invalid task frame state %s, all inputs \
                                    were ready but state said otherwise " %
                                            repr(taskframe))
                else:
                    if next_task is not None:
                        # need to wait for at least one thing
                        if dep_count == 1:
                            # Only depends on the next task we recurse on,
                            # can just treat as continuation
                            continuation.append(task) # previous task
                        else:
                            # This task depends on multiple tasks, add frame to deque  
                            # so other tasks can be run later
                            logging.debug("Saving task frame %s to deque"
                                          % repr(taskframe))
                            self.deque.append(taskframe)
                            # fresh frame with new task
                            continuation = []
                        # build the frame for the next iteration 
                        taskframe = makeframe(next_task, continuation)
                    else:
                        # All dependencies already being executed, so need to suspend
                        logging.debug("Suspending task frame %s until dependencies avail"
                                      % repr(taskframe))
                        raise Exception("BLSDF")
                        # Make callback closure
                        def task_resumer(channel):
                            resume_queue.put(taskframe)
                            
                        for channel in taskframe[2]:
                            if channel is not None:
                                #TODO: assuming atomic?
                                channel._force(done_callback=task_resumer)
                        # exit
                        return None
                        
                # if found_runnable is true, fall out of loop
                first_iter = False
        return taskframe
                    
def isWorkerThread(thread=None):
    """
    If no thread provided, current thread
    """
    if thread is None:
        thread = threading.currentThread()
    name = thread.getName()
    return name[:len(PYFUN_THREAD_NAME)] == PYFUN_THREAD_NAME

def force_recursive(channel):
    """
    Assume we are holding the global mutex when this is called
    """
    thread = threading.currentThread()
    if not isWorkerThread(threading.currentThread()):
        raise Exception("Non-worker thread running force_recursive")
     
    
    # the task to run
    to_run = []
    for task in channel._in_tasks:
        state = task._state
        if state in (T_DATA_READY, T_DATA_WAIT, T_INACTIVE):
            to_run.append(makeframe(task, []))
        elif state in (T_QUEUED, T_RUNNING, T_DONE_SUCCESS):
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
        thread.deque.append(ReturnMarker)
        thread.deque.extend(to_run)
        graph_mutex.release()
        try:
            thread.run(recursive=True)
        finally:
            graph_mutex.acquire()


class DoneContinuation:
    def __init__(self, contstack=None, thread=None):
        if contstack is None or len(contstack) == 0:
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
        logging.debug("%s finished" % repr(task))
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