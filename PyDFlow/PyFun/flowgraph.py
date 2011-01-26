from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.flowgraph import acquire_global_mutex, release_global_mutex
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


def local_exec(task, input_values):
    # Update state so we know its running
    logging.debug("Running task %s with inputs %s" %(repr(task), repr(input_values)))
    acquire_global_mutex()
    task._state = T_RUNNING
    release_global_mutex()

    #TODO: tag failed tasks with exception?
    # In general need to work out failure handling logic
    try: 
        # Run the function in this thread
        return_val = task._func(*(input_values))
    except Exception, e:
        task.state = T_ERROR
        raise Exception("Loclly executed task threw exception %s" % repr(e))
    
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
    #TODO: handle exceptions that occur when setting channel
    

class FuncTask(AtomicTask):
    def __init__(self, func, output_types, input_spec, *args, **kwargs):
        super(FuncTask, self).__init__(output_types, input_spec, *args, **kwargs)
        self._func = func

    def _exec(self):
        logging.debug("Starting a FuncTask")
        #TODO: select execution backend, run me!
        # Build closure for executor
        for o in self._inputs:
            o._prepare(M_READ)
        for o in self._outputs:
            o._prepare(M_WRITE)
        def do():
            local_exec(self, self._gather_input_values())
        LocalExecutor.execute_async(do)


