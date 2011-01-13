from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.flowgraph import acquire_global_mutex, release_global_mutex
from PyDFlow.futures import Future
import PyDFlow.base.flowgraph as gr
from PyDFlow.base.states import *
import LocalExecutor



class FutureChannel(AtomicChannel):

    def __init__(self, *args, **kwargs):
        super(FutureChannel, self).__init__(*args, **kwargs)

    """
    Channel to be used for passing local python variables between functions.
    """
    def _open_bound_read(self):
        """
        Want to be able to bind raw Python values to read from, rather than
        just futures.  So, check to see if the provided value is a future or not
        TODO: what if we wanted to actually operate on futures, is this 'magic' behaviour
        a good thing?
        """
        # replace bound var with future
        if not isinstance(self._bound, Future):
            fut = Future()
            fut.set(self._bound)
            self._bound = fut
        super(FutureChannel, self)._open_bound_read()

def local_exec(task):
    # Update state so we know its running
    acquire_global_mutex()
    task._state = T_RUNNING
    release_global_mutex()

    #TODO: tag failed tasks with exception?
    # In general need to work out failure handling logic
    try: 
        # Run the function in this thread
        return_val = task.func(*(task._input_values))
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
        self.state = T_DONE_SUCCESS
        task.output_channels[0].set(return_val)
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
        self.state = T_DONE_SUCCESS
        for val, chan in zip(return_vals, task._outputs) :
            chan.set(val)
    #TODO: handle exceptions that occur when setting channel
    

class FuncTask(AtomicTask):
    def __init__(self, func, output_types, input_spec, *args, **kwargs):
        super(FuncTask, self).__init__(output_types, input_spec, *args, **kwargs)
        self.func = func
        self.__input_values = None

    def _exec(self):
        #TODO: select execution backend, run me!
        self.__input_values = self._gather_input_values()
        LocalExecutor.execute_async(self)


