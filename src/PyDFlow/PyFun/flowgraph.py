from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.flowgraph import graph_mutex
from PyDFlow.base.states import *
from PyDFlow.futures import *
import LocalExecutor

import logging



class FutureChannel(AtomicChannel):

    def __init__(self, *args, **kwargs):
        super(FutureChannel, self).__init__(*args, **kwargs)
        
        # The bind variable for future channels will always be
        # either a future, or contents for a future
        if self._bound is not None:
            if not isinstance(self._bound, Future):
                # If a non-future data item is provided as the binding,
                # we should pack it into a future (this avoids users)
                # having to write boilerplate to put things into futures
                self._future.set(self._bound)
            else:
                self._future = self._bound
            if self._future.isSet():
                    self._state = CH_DONE_FILLED

    def _has_data(self):
        """
        Check to see if it is possible to start reading from this
        channel now
        """
        return self._future.isSet()

    def _get(self):
        # Make sure a worker thread doesn't block
        #TODO
        print "get!"
        print  LocalExecutor.isWorkerThread()
        print self._state in [CH_OPEN_R, CH_OPEN_RW, CH_DONE_FILLED]
        print self._bound
        print self._in_tasks
        if (not self._state in [CH_OPEN_R, CH_OPEN_RW, CH_DONE_FILLED] )\
                and LocalExecutor.isWorkerThread():
            self._force_by_worker()    
        res = super(FutureChannel, self)._get()
        return res
    
    def _force_by_worker(self):
        """
        Get a worker to iterate down the function graph, running dependent tasks.
        Assumes that has not been filled.
        """
        #for inp in self._in_tasks[1:]:
            # already have lock
         #   inp._force()
        self._force()
        while not self._future.isSet():
            graph_mutex.release()
            try:
                print "running a different task"
                LocalExecutor.run_one_task()
            finally:
                graph_mutex.acquire()
        
        
        
def rec_exec(task):
    pass


def local_exec(task, input_values):
    # Update state so we know its running
    logging.debug("Running task %s with inputs %s" %(repr(task), repr(input_values)))
    #Don't bother grabbing - not critical that update is immediate
    task._state = T_RUNNING

    #TODO: tag failed tasks with exception?
    # In general need to work out failure handling logic
    #try: 
        # Run the function in this thread
    return_val = task._func(*(input_values))
    #except Exception, e:
    #    task.state = T_ERROR
    #    raise Exception("Locally executed task threw exception %s" % repr(e))


    with graph_mutex:
        if return_val is None:
            task.state = T_ERROR
            #TODO: exception type
            raise Exception("Got None return value")
        
        # Fill in all the output channels
        if len(task._outputs) == 1:
            # Update current state, then pass data to channels
            task.state = T_DONE_SUCCESS
            task._outputs[0]._set(return_val)
        else:
            try:  
                return_vals = tuple(return_val)
            except TypeError:
                #TODO: exception types
                task.state = T_ERROR
                raise Exception("Expected tuple or list of length %d as output, but got something not iterable" % (len(task._outputs)))
            if len(return_vals) != len(task._outputs):
                task.state = T_ERROR
                raise Exception("Expected tuple or list of length %d as output, but got something of length" % (len(task._outputs), len(return_vals)))

            # Update current state, then pass data to channels
            task.state = T_DONE_SUCCESS
            for val, chan in zip(return_vals, task._outputs) :
                chan._set(val)
    

class FuncTask(AtomicTask):
    def __init__(self, func, *args, **kwargs):
        super(FuncTask, self).__init__(*args, **kwargs)
        self._func = func

    def _exec(self):
        logging.debug("Starting a FuncTask")
        #TODO: select execution backend, run me!
        # Build closure for executor
        self._prep_channels()
        # grab the input values while we have a lock
        input_values = self._gather_input_values()
        def do():
            local_exec(self, input_values)
        LocalExecutor.execute_async(do)


