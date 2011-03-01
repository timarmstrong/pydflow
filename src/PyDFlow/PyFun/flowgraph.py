from __future__ import with_statement
from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.flowgraph import Unbound
from PyDFlow.base.mutex import graph_mutex
from PyDFlow.base.states import *
from PyDFlow.futures import *
import PyDFlow.base.LocalExecutor as LocalExecutor


import logging
from PyDFlow.types.check import FlTypeError

class FutureChannel(AtomicChannel):

    def __init__(self, *args, **kwargs):
        super(FutureChannel, self).__init__(*args, **kwargs)
        
        # The bind variable for future channels will always be
        # either a future, or contents for a future
        if self._bound is not Unbound:
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
            elif self._state in (T_RUNNING, T_DONE_SUCCESS):
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
        
        # Run the function in this thread.  If the function raises an exception
        # it will be handled by the caller
        return_val = self._func(*(input_values))
            
        with graph_mutex:
            continuation(self, return_val)
        
    def isSynchronous(self):
        return True

    
        
class CompoundTask(AtomicTask):
    def __init__(self, func, *args, **kwargs):
        super(CompoundTask, self).__init__(*args, **kwargs)
        self._func = func
        
    def _exec(self, continuation):
        """
        Just run the task in the current thread, 
        assuming it is ready
        """
        logging.debug("Starting a CompoundTask %s" % repr(self))
        #TODO: select execution backend, run me!
        
        with graph_mutex:
            # Set things up
            # Check state again to be sure it is sensible
            if self._state == T_QUEUED:
                #self._prep_channels()
                self._state = T_RUNNING
            elif self._state in (T_RUNNING, T_DONE_SUCCESS):
                #TODO: safe I think
                return
            else:
                # Bad state
                # TODO: better logic
                raise Exception("Invalid task state %s encountered by worker thread" % 
                                    (task_state_name[self._state]))
        
        # Run the function in this thread.  If the function raises an exception
        # it will be handled by the caller
        return_chans = self._func(*self._inputs)
        try:
            return_chans[0]
            
        except TypeError:
            return_chans = [return_chans]
        if len(return_chans) != len(self._outputs):
                raise FlTypeError(("%s: Mismatch between number of channels returned by function " +
                                   " %s and number of channels expected: %d ") % (repr(self),
                                        repr(return_chans), len(self._outputs)))
        with graph_mutex:
            for old, new in zip(self._outputs, return_chans):
                old <<= new 
            
            
    def isSynchronous(self):
        return True