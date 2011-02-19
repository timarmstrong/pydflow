import base
from base import FutureChannel
import threading
import swtypes
import python_executor
import inspect

import logging

swfuture = FutureChannel

class func(base.task_decorator):
    """
    The func decorator.  When applied to a python function, it can be
    the function can be used as a "task".

    The func decorator will validate the input types of the  function,
    and will tag the output of the function with the type

    E.g. 
    @func((Int.sum), (Int.a, Int.b))
    def add (a, b):
        return a + b
    """
    def __init__(self, output_types, input_types):
        super(func, self).__init__(output_types, input_types)
        self.task_class = FuncTask
        self.function_converter = lambda x: x

                
class FuncTask(base.AtomicTask):
    def __init__(self, converted_function, output_types, input_spec, 
                    *args, **kwargs):
        """
        __init__ should set up the task ready for execution.
        input_spec: specification of task input types
        output_types: list task output types
        converted_function: a processed representation of the function that
                has just been decorated
        args, kwargs: the input channel args
        """
        super(FuncTask, self).__init__(output_types, 
                                input_spec, *args, **kwargs)
        self.function = converted_function
        
    def _run(self):
        # Don't want to execute in this thread, so create a closure and hand
        # over to a worker thread.  This will only be run once the input channels
        # are prepared
        logging.debug("Task #%d _run() called" % self.task_num)
        def run_func():
            # Note: data is being passed indirectly to this function
            self.lock()
            self.state = base.STATE_RUNNING # Update state to reflect that it is actually executing now
            self.unlock()
            logging.debug("Task #%d collecting input data from %d channels" % (
                self.task_num, len(self.input_channels)))
            
            input_data = self._gather_input_data()
            logging.debug("Task #%d running, got all input data ok" % self.task_num)
            return_val = self.function(*input_data) 
            if return_val is None:
                raise SWTypeError("Got None return value")
            logging.debug("Task #%d successfully completed" % self.task_num)
            
            # Fill in all the output channels
            if len(self.output_channels) == 1:
                self.output_channels[0].set(return_val)
            else:
                try:  
                    return_vals = tuple(return_val)
                except TypeError:
                    raise SWTypeError("Expected tuple or list of length %d as output, but got something not iterable" % (len(self.output_types)))
                if len(return_vals) != len(self.output_types):
                    raise SWTypeError("Expected tuple or list of length %d as output, but got something of length" % (len(self.output_types), len(return_vals)))
                for val, chan in zip(return_vals, self.output_channels) :
                    chan.set(val)
            self.state = base.STATE_FINISHED
        # hand over to worker thread
        python_executor.execute_async(run_func)
    
