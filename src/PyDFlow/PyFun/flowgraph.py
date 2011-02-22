from __future__ import with_statement
from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.flowgraph import graph_mutex
from PyDFlow.base.states import *
from PyDFlow.futures import *
import PyDFlow.base.LocalExecutor as LocalExecutor


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
        if (not self._state in [CH_OPEN_R, CH_OPEN_RW, CH_DONE_FILLED] )\
                and LocalExecutor.isWorkerThread():
            self._force_by_worker()    
        res = super(FutureChannel, self)._get()
        return res
        
    

class FuncTask(AtomicTask):
    def __init__(self, func, *args, **kwargs):
        super(FuncTask, self).__init__(*args, **kwargs)
        self._func = func



    def _exec(self, continuation):
        """
        Just run the task in the current thread, 
        assuming it is ready
        """
        logging.debug("Starting a FuncTask %s" % repr(self))
        #TODO: select execution backend, run me!
        
        with graph_mutex:
            # Set things up
            # Check state again to be sure it is sensible
            if self._state == T_QUEUED:
                self._prep_channels()
                input_values = self._gather_input_values()
                self._state = T_RUNNING
            elif self._state in [T_RUNNING, T_DONE_SUCCESS]:
                #TODO: safe I think
                return
            else:
                # Bad state
                # TODO: better logic
                raise Exception("Invalid task state %s encountered by worker thread" % 
                                    (task_state_name[self._state]))
        
        # Update state so we know its running
        #logging.debug("Running %s with inputs %s" %(repr(self), repr(input_values)))
    
        #TODO: tag failed tasks with exception?
        # In general need to work out failure handling logic
        try: 
            # Run the function in this thread
            return_val = self._func(*(input_values))
            #logging.debug("%s returned %s" %(repr(self), repr(return_val)))
        except Exception, e:
            with graph_mutex:
                self.state = T_ERROR
                raise Exception("Locally executed task threw exception %s" % repr(e))
            
        with graph_mutex:
            continuation(self, return_val)
        
        
