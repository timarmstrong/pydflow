"""
Implements the FlowGraph, a bipartite graph of Tasks and Channels which
represents the program to be executed.

Naming conventions:
For Task and Channel derivatives, a method or field prefixed with __ is intended
for internal use only and may reqire a lock to be held when calling.

A method prefixed with _ is intended to be overridden

A method or field without such a prefix is intended for external use, and will not
assume that any looks are held.
"""

import PyDFlow.futures
import logging
from PyDFlow.types import fltype

from threading import Lock


graph_mutex = Lock()

def acquire_global_mutex():
    global graph_mutex
    graph_mutex.acquire()

def release_global_mutex():
    global graph_mutex
    graph_mutex.release()


#==============#
# TASK STATES  #
#==============#
T_INACTIVE, T_DATA_WAIT, T_DATA_READY, T_QUEUED, T_RUNNING, \
        T_DONE_SUCCESS, T_ERROR = range(7)

#=================#
# CHANNEL STATES  #
#=================#
CH_CLOSED, CH_CLOSED_WAITING, \
        CH_OPEN_W, CH_OPEN_RW, CH_OPEN_R, CH_DONE_FILLED, CH_DONE_DESTROYED, \
        CH_ERROR = range(8)

#TODO: garbage collection state: destroy the data if no output tasks depend
# on it?



class Task(object):
    def __init__(self, output_types, input_spec, *args, **kwargs):
        # Set the state to this value, however it can be overwritten
        # by child classes, as it migh tbe possible for some task
        # types to start running if not all channels are ready
        if len(input_spec) == 0:
            self.__state = T_DATA_READY
        else: 
            self.__state = T_INACTIVE

        # Validate the function inputs.  note: this will consume
        # kwargs that match task input arguments
        self.__inputs = validate_inputs(input_spec, args, kwargs)
        graph_mutex.acquire()
        try: 
            self.__setup_inputs(self, input_spec)
            self.__setup_outputs(self, output_types, **kwargs)
        finally:
            graph_mutex.release()
        graph_mutex.release()
    
    def __setup_inputs(self, input_spec):
        global graph_mutex
        self.__input_spec = input_spec
        not_ready_count = 0
        inputs_ready = []
        for c, s in zip(self.__inputs, input_spec):
            ready = s.isRaw() or c.register_output(self)
            inputs_ready.append(ready)
            if not ready: 
                not_ready_count += 1
        self.__inputs_notready_count = not_ready_count
        self.__inputs_ready = inputs_ready

    def __all_inputs_ready(self):
        return self.__inputs_notready_count == 0

    def __setup_outputs(self, output_types, **kwargs):
        outputs = None
        # Setup output channels
        if "_outputs" in kwargs:
            outputs = kwargs["_outputs"]
        elif "_out" in kwargs:
            outputs = kwargs["_out"]

        if outputs is not None:
            # Pack into a tuple if needed
            if not isinstance(outputs, (list, tuple)):
                outputs = (outputs,)
            if len(outputs) != len(output_types):
                raise Exception("_outputs must match length of output_types")
            err = [(chan, t) for chan, t in zip(outputs, output_types)
                    if not t.isinstance(chan)]
            if err:
                raise Exception("Output channel of wrong type provided")
        else:
            outputs = [channel_cls() for channel_cls in output_types]
        self.__outputs = outputs

        # Connect up this task as an output for the channel
        for o in outputs:
            o.register_input(self)

    def __startme(self):
        """
        Function which starts this task running, after inputs have become enabled
        as appropriate
        """
        #TODO: notify channels, etc
        self.__state = T_QUEUED
        self._exec()

    def _exec(self):
        """
        To be overridden in child class
        Start the task running and return immediately.
        """
        raise UnimplementedException("_exec is not implemented") 

    def _input_fail(self, input):
        """
        Can be overridden. Called only when there is some bad failure in hte
        input channel, in order to allow error states to propagate.
        """
        self.__state = T_ERROR
        #TODO: send error to output channels
    
    def _input_readable(self, input, oldstate, newstate):
        """
        To be overridden in child class.  Called when an input changes
        state to one where there is some valid data in the channel from
        one in which nothing could be read in the channel.
        Determines what happens when an input channel changes state.
        """
        # Check may be overly defensive here
        ix = self.__inputs.index(input)
        if not self.__inputs_ready[i]:
            self.__inputs_notready_count -= 1
            self.__inputs_ready[i] = True

    def force(self):
        """
        Ensure that at some point in the future this task will start running.
        I.e. if it is runnable, start it running, otherwise make sure that
        the task's inputs will be filled.
        """
        global graph_mutex
        graph_mutex.acquire()
        try: 
            if self.__state == T_DATA_READY:
                # Just start it running
                self.__startme()
                graph_mutex.release()
            elif self.__state == T_INACTIVE:
                # Force all inputs
                self.__state = T_DATA_WAIT
                for inp, spec in zip(self.__inputs, self.__input_spec):
                    if not spec.isRaw():
                        # if input is filled or already filling, method 
                        # should do nothing
                        inp.force() 
                graph_mutex.release()
        finally:
            graph_mutex.release()
    
    def state(self):
        """
        Return the current state of the task
        """
        global graph_mutex
        graph_mutex.acquire()
        res = self.__state
        graph_mutex.release()
        return res


class Channel(fltype):
    """
    Channels form the other half of the bipartite graph, alongside tasks.
    This class is an abstract base class for all other channels to derive 
    from
    """
    def __init__(self, __bind_location=None):
        """
        bound_to is a location that the data will come from
        or will be written to.
        """
        self.__in_tasks = []
        self.__out_task = []
        self.__state = CH_CLOSED
        self.__bound = __bind_location
    
    
    # Channel modes for prepare call
    M_READ, M_WRITE, M_READWRITE = range(3)
    def _prepare(self, mode):
        """
        Called by an input or output task to indicate that the
        channel should get ready to be read from/written to.

        It should only be called if there is a valid transition 
        from the current state to CH_OPEN_* as appropriate, ie.
        so that the channel becomes immediately open.
        TODO: intermediate states if preparation takes some time?

        TODO: change state as appropriate?
        should default implementaion just be nice and change the
        state as needed?
        """
        pass
        #TODO
    
    def _register_input(self, input_task):
        """
        called when a task wants to write to this channel
        Returns True if the channel is ready to be written to

        Default implementation simply adds to list of input
        tasks regardless.  Should be overridden with an
        implementation that checks that states are legal, etc.

        TODO: more sophisticated implementation?
        """
        self.__in_tasks.append(input_task)

    def _register_output(self, output_task):
        """
        called when a task wants to read from this channel.
        Returns True if the channel is ready to be read from

        Default implementation simply adds to list of input
        tasks regardless.  Should be overridden with an
        implementation that checks that states are legal, etc.
        
        TODO: more sophisticated implementation?
        """
        self.__out_tasks.append(output_task)

    def add_input(self, input_task):
        global graph_mutex
        graph_mutex.acquire()
        self._register_output(output_task)
        graph_mutex.release()

    def add_output(self, input_task):
        global graph_mutex
        graph_mutex.acquire()
        self._register_input(input_task)
        graph_mutex.release()

    def get(self):
        """
        TODO: doco
        """
        raise UnimplementedException("get not implemented on base Channel class")

    def force(self):
        """
        TODO: document
        Should be overridden.  The overriding method definition should:
        check states of inputs, update own state, start any inputs running
        """
        raise UnimplementedException("force not implemented on base Channel class")
    
    def state():
        """
        Return the current state of the channel
        """
        global graph_mutex
        graph_mutex.acquire()
        res = self.__state
        graph_mutex.release()
        return res

    @classmethod
    def bind(cls, location):
        """
        Creates a new instance of the class, bound to some underlying data
        object or location.
        """
        return cls(__bind_location=location)


