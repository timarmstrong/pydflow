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

@author: Tim Armstrong
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
QUEUE_TIMEOUT = 0.02

# Work first arrives in this queue from other threads
in_queue = None

#==============================================#
# RESUME QUEUE
#==============================================#
resume_queue = None

# ========================================== #
#  WORKER THREADS
# ===========================================#

PYFUN_THREAD_NAME = "pyfun_thread"

#NUM_THREADS = 8
NUM_THREADS = 4

workers = []

# These deques store the work remaining to be done
work_deques = None
ReturnMarker = object()

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

def exec_async(channel):
    """
    Takes a channel and fills it at some point in the future
    """
    # Ensure workers are initialized
    logging.debug("Entered exec_async")
    initFuture.get()
    logging.debug("Added channel to work queue")
    in_queue.put(channel)
    with idle_worker_cvar:
        idle_worker_cvar.notifyAll()

def fail_task(task, continuation, exceptions):
    task._fail(exceptions)
    if continuation is not None:
        for t in continuation:
            t._fail(exceptions)

def makeframe(channel, continuation):
    """
    Make a deque frame from a task. Graph mutex should be held.
    """
    deps = []
    if channel._in_tasks is not None:
        for task in channel._in_tasks:
            for spec, ch in task._input_iter():
                #TODO: support lazy/strict
                if not spec.isRaw() and not ch._try_readable():
                    if ch._state == CH_ERROR:
                        fail_task(task, continuation, ch._exceptions)
                        return None
                    deps.append(ch)
    return (channel, deps, continuation)

class WorkerThread(threading.Thread):
    """ 
    Worker thread class: repeatedly grabs callable items
    from work_queue, and runs them.
    
    The internal deque is a thread-safe double ended queue designed to facilitate
    work-stealing.  It contains suspended tasks that are candidates for workstealing.
    
    Each internal element has the following structure:
        (channel_that_needs_filling, dependency_list, continuation_list)
        channel_that_needs_filling: a channel object which has been suspended due to dependencies
        dependency_list: a list of input channels which need to be filled in order to allow
                    task to run to file the above channel
                    each entry is replaced by None when finished.
        continuation_list: a sequence of tasks that can be run from last-to-first once
                the channel is filled
    """
    def __init__(self, in_queue, resume_queue, worker_num, work_deque):
        threading.Thread.__init__(self, name=("%s_%d" % (PYFUN_THREAD_NAME, worker_num)))
        self.in_queue = in_queue
        self.resume_queue = resume_queue
        
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
            frame = self.get_from_queue(self.resume_queue, random.random() > 0.5, QUEUE_TIMEOUT * random.random(), frame=True)
            if frame is not None:
                self.eval_frame(frame)
                logging.debug("%s ran from resume" % self.getName())
                continue
            
            logging.debug("%s try to run from new work queue" % self.getName())
            # Try to get work from global queue (until timeout) 
            frame = self.get_from_queue(self.in_queue, random.random() > 0.5, 
                                        QUEUE_TIMEOUT * random.random(),frame=False)
            if frame is not None:
                logging.debug("%s running from new work queue" % self.getName())
                self.eval_frame(frame)
                logging.debug("%s ran from new work queue" % self.getName())
                continue
            else:
                logging.debug("frame was none")
            
            logging.debug("%s try to steal work" % self.getName())
            # Try to steal work
            if self.steal_work():
                logging.debug("%s stole work" % self.getName())
                continue
            
            # If there was no work, see if we can establish consensus 
            # among threads that there is no work to do
            #logging.debug("%s trying to go idle, found no work" % self.getName())
            frame = self.try_idle()
            if frame is not None:
                self.eval_frame(frame)
            
    def exec_task(self, frame):
        """
        Start the task running.  State should eb set to T_QUEUED
        """
        chan = frame[0]
        # assume for now <= 1, can revisit assumption later
        # It shuoldn't be 0, because we shouldn't be executing
        # task if it gets to that point
        assert(len(chan._in_tasks) == 1) 
        task = chan._in_tasks[0]
        contstack = frame[2]

        logging.debug("%s starting task %s" % (self.getName(), repr(task)))
        assert(task._state == T_QUEUED)
        #TODO: what if continuation called before exception
        if task.isSynchronous():
            callback = lambda task, return_val: done_continuation(task, return_val, None)
            
            try:
                task._exec(callback)
            except Exception, e:
                logging.error("%s caught exception %s" % (self.getName(), 
                                            repr(e)))
                fail_task(task, contstack, [e])
                return
            error = None
            if contstack is not None:
                for nexttask in reversed(contstack):
                    if error is None:
                        try:
                            nexttask._exec(callback)
                        except Exception, e:
                            error = e
                    if error is not None:
                        fail_task(nexttask, [], [error])
        else:
            try:
                task._exec(done_continuation, contstack)
            except Exception, e:
                fail_task(task, contstack, [e])
            
    def run_from_deque(self):
        try:
            frame = self.deque.pop()
            if frame is ReturnMarker:
                return False
            self.eval_frame(frame)
            return True
        except IndexError:
            logging.debug("Thread %s - nothing in Deque" % (self.getName()))
            return False
        
    
    def get_from_queue(self, queue, block, timeout, frame):
        """
        Trys to get some new work from the queue provided
        Returns True if found, false otherwise
        """
        try:
            item = queue.get(block, timeout)
            
            queue.task_done()
            if frame:
                frame = item
            else:
                with graph_mutex:
                    # Build the taskframe: the task and all of its unresolved
                    # dependencies
                    frame = makeframe(item, [])
            
            logging.debug("%s got %s from queue" % (threading.currentThread().getName(),
                                                    repr(frame)))
            # Note: makeframe could have returned none if there was an error
            # processing the input... we just need to return None
            # to let error propagate 
            return frame
        except Queue.Empty:
            logging.debug("Thread %s queue timeout" % (self.getName()))
            return None
        
    def try_idle(self):
        """
        Try to see if work is exhausted and threads can go idle.
        The threads keep on polling until there is a consensus
        that there is no work.
        """
        global idle_worker_count, idle_worker_cvar
        frame = None
        with idle_worker_cvar:
            if not self.idle:
                self.idle = True
                idle_worker_count += 1
            while idle_worker_count == NUM_THREADS:
                # all threads idle
                # with idle_worker_cvar, try to get something from 
                # queue, if there is genuinely nothing there, block on
                # condition
                
                frame = self.get_from_queue(self.resume_queue, False, None, frame=True)
                if frame is not None:
                    # got a task: wake up all
                    self.idle = False
                    idle_worker_count = 0
                    idle_worker_cvar.notifyAll()
                    break
                
                frame = self.get_from_queue(self.in_queue, False, None, frame=False)
                if frame is not None:
                    self.idle = False
                    idle_worker_count = 0
                    idle_worker_cvar.notifyAll()
                    break
                
                idle_worker_cvar.wait()
        self.idle = False
        if frame is not None:
            self.eval_frame(frame)
    
    def steal_work(self):
        """
        Attempts to steal work using a round robin scheme.
        Returns True if succeeds in stealing one task
        returns False if unsuccessful
        """
        thread_ids = range(NUM_THREADS - 1)
        random.shuffle(thread_ids)
        # try each of the threads in a randomised order
        for victim in thread_ids:
            if victim >= self.worker_num:
                # adjust so that don't try to steal from self
                victim += 1
            try:
                frame = work_deques[victim].popleft()
                while frame is ReturnMarker:
                    frame = work_deques[victim].popleft()
                # Run and then go back to normal loop
                logging.debug("Thread %s stole frame %s from thread #%d" % (
                            self.getName(), repr(frame), victim))
                self.eval_frame(frame)
                return True
            except IndexError:
                pass
        logging.debug("Thread %s couldn't find work to steal" % (self.getName()))
        return False
            
    def eval_frame(self, frame):
        """
        frame is: (channel, 
                [channel dependencies which may not have been picked by an evaluator],
                [list of tasks to run following immediately])
         If ready to run, this thread runs the task
         If not, it searches the graph until it finds a runnable task, adding branches to the deque. 
        """        
    
        logging.debug("%s: looking at frame %s " % (self.getName(), repr(frame)))
        ch = frame[0]
                
        graph_mutex.acquire()
        # Expand the channel as needed
        while hasattr(ch, '_proxy_for'):
            #TODO: handle non-lazy arguments
            ch = ch._expand()
            logging.debug("expanded channel to %s " % repr(ch))
        frame = (ch, frame[1], frame[2])
        
        chstate = ch._state
        if chstate in (CH_DONE_FILLED, CH_OPEN_W, CH_OPEN_R, CH_OPEN_RW):
            # Channel is filled or will be filled by something else 
            graph_mutex.release()
            return
        elif chstate == CH_ERROR:
            # Exception should already be propagated, just return
            graph_mutex.release()
            return
        elif chstate in (CH_CLOSED, CH_CLOSED_WAITING):
            # Continue on to evaluate
            pass
        else:
            raise Exception("Invalid channel state for %s" % repr(frame))
        
        #TODO: check channel state
        assert(len(ch._in_tasks) == 1)
        task = ch._in_tasks[0]
        state = task._state
        if state == T_DATA_READY:
            task._state = T_QUEUED
            graph_mutex.release()
            #  All dependencies satisfied
            self.exec_task(frame)
        elif state in (T_DONE_SUCCESS, T_RUNNING, T_QUEUED, T_CONTINUATION):
            # already started elsewhere
            graph_mutex.release()
            return
        elif state in (T_DATA_WAIT, T_INACTIVE):
            task._state = T_DATA_WAIT
            try:
                runnable_frame = self.find_runnable_task(frame)
                if runnable_frame is not None:
                    assert(len(runnable_frame[0]._in_tasks) == 1) 
                    runnable_frame[0]._in_tasks[0]._state = T_QUEUED
            finally:
                graph_mutex.release()
            if runnable_frame is not None:
                self.exec_task(runnable_frame)
                
        elif state == T_ERROR:
            # Exception should already be propagated, just return
            graph_mutex.release()
            return
        else:
            graph_mutex.release()
            raise Exception("Task %s has invalid state %d" % ((repr(task), state)))
    
    
    def find_runnable_task(self, taskframe):
        """        
        This does a depth first search until it finds a runnable task and leaves unexplored
        branches in frames on the deque.
        
        It gathers all of the tasks which have only a single dependency.
        
        Returns a task frame with a channel with input task state T_DATA_READY, and continuation
        with all tasks set to T_CONTINUATION
        or None if nothing to run
        """
        logging.debug("%s: find_runnable_task" % (self.getName()))
        found_runnable = False
        first_iter = True
        
        while not found_runnable:
            dep_count = 0 # number of tasks that need to finish before this task can run
            ch, deps, continuation = taskframe
            
            # Shouldn't get here unless channel has input tasks
            assert(len(ch._in_tasks) == 1)
            task = ch._in_tasks[0]
            next_ch = None
            
            # Looks at inputs that might not yet be ready
            for i, ch in enumerate(deps):
                # TODO: check for placeholder channel and expand it at this point
                # TODO: this is assuming atomic channel
                # don't need to check if we just created frame while we were holding lock
                if ch is None:  
                    continue
                elif ch._state == CH_ERROR:
                    fail_task(task, continuation, ch._exceptions)
                    continue
                elif hasattr(ch, '_proxy_for'):
                    ch = ch._expand()
                    while hasattr(ch, '_proxy_for'):
                        ch = ch._expand()
                    logging.debug("expanded channel to %s " % repr(ch))
                    deps[i] = ch
                # Recheck state
                if ch._readable():                          
                    # is ready.. don't do anything
                    logging.debug("%s became readable" % repr(ch))
                    deps[i] = None
                    continue
                elif ch._state == CH_ERROR:
                    fail_task(task, continuation, ch._exceptions)
                    continue
                elif ch._state == CH_CLOSED:
                    ch._state = CH_CLOSED_WAITING 
                
                # Next task to look at
                logging.debug("%s, %s" % (repr(ch), repr(ch._in_tasks)))
                assert(len(ch._in_tasks) == 1)
                in_t = ch._in_tasks[0]
                state = in_t._state
                if state == T_INACTIVE:
                    # We will recurse on this one
                    if next_ch is None:
                        # do dfs on this task to run dependencies
                        in_t._state = T_DATA_WAIT
                        next_ch = ch
                    dep_count += 1
                elif state == T_DATA_READY:
                    # This task could be run now
                    if next_ch is None:
                        logging.debug("selected %s to run for thread %s" % 
                                  (repr(in_t), self.getName()))
                        next_ch = ch
                        found_runnable = True
                    dep_count += 1
                elif state == T_DONE_SUCCESS:
                    pass
                elif state in (T_DATA_WAIT, T_RUNNING, T_QUEUED, T_CONTINUATION):
                    # Will be filled with data at some point by another executor
                    # but we do need to wait for it to finish
                    dep_count += 1
                else:
                    raise Exception("Invalid task state for %s" %
                                    (repr(taskframe)))
            
            logging.debug("Depends on %d more channels, next channel is %s" % (dep_count, next_ch))
            if dep_count == 0:
                if task._state == T_DATA_READY:
                    # it is possible that the task became runnable if it was a compound
                    found_runnable = True
                else: 
                    raise Exception(("Invalid task state %s with frame %s, all inputs " +
                                     "were ready but state said otherwise ") %
                                        (repr(task), repr(taskframe)))
            else:
                if next_ch is not None:
                    # need to wait for at least one thing
                    if dep_count == 1:
                        # Only depends on the next task we recurse on,
                        # can just treat as continuation
                        task._state = T_CONTINUATION
                        continuation.append(task) # previous task
                    else:
                        # This task depends on multiple tasks, add frame to deque  
                        # so other tasks can be run later
                        logging.debug("%s: saving task frame %s to deque"
                                      % (self.getName(), repr(taskframe)))
                        self.deque.append(taskframe)
                        # fresh frame with new task
                        continuation = []
                    # build the frame for the next iteration 
                    taskframe = makeframe(next_ch, continuation)
                    
                    # as part of making frame, task state can change
                    if next_ch._state == T_DATA_READY:
                        found_runnable = True
                else:
                    # All dependencies already being executed, so need to suspend
                    logging.debug("Suspending task frame %s until dependencies avail"
                                  % repr(taskframe))
                    #TODO: really need to think about how this will work in presence of multiple
                    #     channels
                    # constraints:
                    #  - task state should be set to T_DATA_WAIT so that no other evaluators will
                    #    attempt to modify it
                    #  - multiple threads may finish evaluating channels at same time
                    #  - other evaluators may be evaluating channels at same time
                    #  - 
                    
                    # Make callback closure
                    def task_resumer(channel):
                        # graph mutex will be held when this called
                        try:
                            ch_index = taskframe[1].index(channel)
                            taskframe[1][ch_index] = None
                        except ValueError:
                            pass
                        for ch in taskframe[1]:
                            if ch is not None and not ch._readable():
                                logging.debug("task resumer can't yet resume taskframe:%s" % (
                                                        repr(taskframe)))
                                return
                        logging.debug("task resumer resuming taskframe:%s" % (repr(taskframe)))
                        resume_taskframe(taskframe)
                        
                    for channel in taskframe[1]:
                        if channel is not None:
                            #TODO: assuming atomic?
                            # All channels are already being evaluated by other threads:
                            #     just register for notifications
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
    logging.debug("force_recursive %s" % thread.getName())
    if not isWorkerThread(threading.currentThread()):
        raise Exception("Non-worker thread running force_recursive")
     
    
    #assert(len(channel._in_tasks) == 1)
    #task = channel._in_tasks[0]
    state = channel._state
    if state in (CH_CLOSED, CH_DONE_DESTROYED):
        frame = makeframe(channel, [])
        must_run = True
    elif state in (CH_CLOSED_WAITING, CH_DONE_FILLED, CH_OPEN_RW, CH_OPEN_W):
        must_run = False
    elif state == CH_ERROR:
        # exception needs to percolate up to next task running 
        raise Exception("%s encountered an error in recursive invocation" % 
                                                                repr(channel))
    else:
        raise Exception("%s had invalid state" % repr(channel))
    

    if not must_run:
        # No work to do, return
        return
    else: 
        # add marker to deque to ensure we don't
        # start executing old tasks
        thread.deque.append(ReturnMarker)
        #thread.deque.extend(to_run)
        thread.deque.append(frame)
        graph_mutex.release()
        try:
            thread.run(recursive=True)
        finally:
            graph_mutex.acquire()


def done_continuation(task, return_val, contstack):
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
    if contstack is not None and len(contstack) > 0:
        contstack[0]._state = T_DATA_READY # revert from T_CONTINUATION
        with idle_worker_cvar:
            resume_queue.put((contstack[0]._outputs[0], None, contstack[1:]))
            idle_worker_cvar.notifyAll()

            
def resume_taskframe(taskframe):
    with idle_worker_cvar:
        logging.debug("Put %s on resume queue" % repr(taskframe))
        resume_queue.put(taskframe)
        idle_worker_cvar.notifyAll()
            
