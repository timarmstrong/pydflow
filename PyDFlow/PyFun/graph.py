from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.flowgraph import acquire_global_mutex, release_global_mutex
import PyDFlow.base.flowgraph as gr
import LocalExecutor



class FutureChannel(AtomicChannel):
   """
    TODO: override anything? maybe Atomicchannel can just be used easily
    for local vars
   """

def local_exec(task):
    # Update state so we know its running
    acquire_global_mutex()
    task.__state = gr.T_RUNNING
    release_global_mutex()

    #TODO: tag failed tasks with exception?
    # In general need to work out failure handling logic
    try: 
        # Run the function in this thread
        return_val = task.func(*(task.__input_values))
    except Exception, e:
        task.state = gr.T_ERROR
        raise Exception("Loclly executed task threw exception %s" % repr(e))
    
    if return_val is None:
        task.state = gr.T_ERROR
        #TODO: exception type
        raise Exception("Got None return value")
    
    # Fill in all the output channels
    if len(task.__outputs) == 1:
        # Update current state, then pass data to channels
        self.state = gr.T_DONE_SUCCESS
        task.output_channels[0].set(return_val)
    else:
        try:  
            return_vals = tuple(return_val)
        except TypeError:
            #TODO: exception types
            task.state = gr.T_ERROR
            raise Exception("Expected tuple or list of length %d as output, but got something not iterable" % (len(task.__outputs)))
        if len(return_vals) != len(task.__outputs):
            task.state = gr.T_ERROR
            raise Exception("Expected tuple or list of length %d as output, but got something of length" % (len(task.__outputs), len(return_vals)))

        # Update current state, then pass data to channels
        self.state = gr.T_DONE_SUCCESS
        for val, chan in zip(return_vals, task.__outputs) :
            chan.set(val)
    #TODO: handle exceptions that occur when setting channel
    

class FuncTask(AtomicTask):
    def __init__(self, func, output_types, input_spec, *args, **kwargs):
        super(FuncTask, self).__init__(output_types, input_spec, *args, **kwargs)
        self.func = func
        self.__input_values = None

    def _exec(self):
        #TODO: select execution backend, run me!
        self.__input_values = self.__gather_input_values()
        LocalExecutor.execute_async(self)


