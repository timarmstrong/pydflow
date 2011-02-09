"""
Implements the FlowGraph, a bipartite graph of Tasks and Channels which
represents the program to be executed.

Naming conventions:
For Task and Channel derivatives, a method or field prefixed with _ is intended
for internal use only and may reqire a lock to be held when calling.  A method 
prefixed with _ may be intended to be overridden

A method or field without such a prefix is intended for external use, and will not
assume that any looks are held.
"""

import PyDFlow.futures
import logging
from PyDFlow.types import flvar 
from PyDFlow.types.check import validate_inputs, spec_zip

from states import *
from exceptions import *

from threading import Lock
import time


graph_mutex = Lock()

def acquire_global_mutex():
    global graph_mutex
    graph_mutex.acquire()

def release_global_mutex():
    global graph_mutex
    graph_mutex.release()

# Keep track of how many tasks we have started without yield
yield_counter = 0
YIELD_INTERVAL = 50

def doyield():
    global yield_counter
    # Give executor a chance to grab global lock
    if yield_counter >= YIELD_INTERVAL:
        yield_count = 0
        global graph_mutex
        graph_mutex.release()
        logging.debug("_force yielded")
        time.sleep(0) # Activate scheduler
        graph_mutex.acquire()
    else:
        yield_counter += 1


class Task(object):
    def __init__(self, output_types, input_spec, *args, **kwargs):
        # Set the state to this value, however it can be overwritten
        # by child classes, as it migh tbe possible for some task
        # types to start running if not all channels are ready
        taskname = kwargs.pop('_taskname', None)
        if len(input_spec) == 0:
            self._state = T_DATA_READY
        else: 
            self._state = T_INACTIVE
        
        self._taskname = taskname

        # Validate the function inputs.  note: this will consume
        # kwargs that match task input arguments
        self._inputs = validate_inputs(input_spec, args, kwargs)
        graph_mutex.acquire()
        try: 
            self.__setup_inputs(input_spec)
            self.__setup_outputs(output_types, **kwargs)
        finally:
            graph_mutex.release()
    
    def __repr__(self):
        return "<PyDFlow Task Instance: %s>" % repr(self._taskname)


    def __setup_inputs(self, input_spec):
        self._input_spec = input_spec
        not_ready_count = 0
        inputs_ready = []
        for c, s in self._input_iter():
            ready = s.isRaw() or c._register_output(self)
            inputs_ready.append(ready)
            if not ready: 
                not_ready_count += 1
        logging.debug("%d inputs, %d not ready for task %s" % (
                len(self._inputs), not_ready_count, repr(self)))
        self._inputs_notready_count = not_ready_count
        self._inputs_ready = inputs_ready

    def _all_inputs_ready(self):
        return self._inputs_notready_count == 0

    def _input_iter(self):
        """
        Iterate through inputs and corresponding spec items.
        This handles the complexity of one spec corresponding to
        multiple items.
        """
        return spec_zip(self._inputs, self._input_spec) 

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
        self._outputs = outputs

        # Connect up this task as an output for the channel
        for o in outputs:
            o._register_input(self)

    def _startme(self):
        """
        Function which starts this task running, after inputs have become enabled
        as appropriate
        """
        #TODO: notify channels, etc
        self._state = T_QUEUED
        logging.debug("Enqueued task %s" % repr(self))
        #print "_exec called"
        self._exec()
        doyield()

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
        self._state = T_ERROR
        #TODO: send error to output channels
    
    def _input_readable(self, input, oldstate, newstate):
        """
        To be overridden in child class.  Called when an input changes
        state to one where there is some valid data in the channel from
        one in which nothing could be read in the channel.
        Determines what happens when an input channel changes state.
        """
        # Check may be overly defensive here
        for ix, inp in enumerate(self._inputs):
            if inp == input:
                if not self._inputs_ready[ix]:
                    self._inputs_notready_count -= 1
                    self._inputs_ready[ix] = True

    def _output_replace(self, old, new):
        """
        Swap out an old output channel with a new one.
        """
        for i, o in enumerate(self._outputs):
            if o is old:
                self._outputs[i] = new

    def force(self):
        """
        Ensure that at some point in the future this task will start running.
        I.e. if it is runnable, start it running, otherwise make sure that
        the task's inputs will be filled.
        """
        global graph_mutex
        graph_mutex.acquire()
        try: 
            self._force()
        finally:
            graph_mutex.release()

    def _force(self):
        logging.debug("Task %s forced, state is %d" % (repr(self), self._state))
        if self._state == T_DATA_READY:
            # Just start it running
            logging.debug("Task %s forced: running immediately" % repr(self))
            self._startme()
        elif self._state == T_INACTIVE:
            logging.debug("Task %s forced: starting dependencies" % repr(self))
            # Force all inputs
            self._state = T_DATA_WAIT
            for inp, spec in self._input_iter():
                if not spec.isRaw():
                    # if input is filled or already filling, method 
                    # should do nothing
                    inp._force() 
        else:
            logging.debug("Task %s forced: no action needed" % repr(self))
    
    def state(self):
        """
        Return the current state of the task
        """
        global graph_mutex
        graph_mutex.acquire()
        res = self._state
        graph_mutex.release()
        return res


class Channel(flvar):
    """
    Channels form the other half of the bipartite graph, alongside tasks.
    This class is an abstract base class for all other channels to derive 
    from
    """
    def __init__(self, _bind_location=None):
        """
        bound_to is a location that the data will come from
        or will be written to.
        """
        super(Channel, self).__init__()
        self._in_tasks = []
        self._out_tasks = []
        self._state = CH_CLOSED
        self._bound = _bind_location
        self._done_callbacks = []
        self._reliable = False
   
    def __ilshift__(self, oth):
        """
        "Assigns" the channel on the right hand side to this one.
        More accurately, merges the data from the RHS channel
        into the LHS channel, 
        making sure that the data from the RHS channel
        is redirected to the LHS channel and also that channel
        retains all the same settings as the LHS channel.
        """
        #TODO: logic
        global graph_mutex
        graph_mutex.acquire()
        oth._replacewith(self)
        graph_mutex.release()
        return self
    
    def __irshift__(self, oth):
        """
        Same as ilshift but injecting LHS into RHS
        """
        global graph_mutex
        graph_mutex.acquire()
        self._replacewith(oth)
        graph_mutex.release()
        return oth

    def _replacewith(self, other):
        """
        Replace this channel with a different channel in all
        input tasks. We can assume that the other channel is
        freshly created and no references are held to it.
        The default behaviour will be to keep all the data 
        and state associated with this channel, but make sure
        that all tasks with output going into other are redirected 
        here.
        """
        for t in self._in_tasks:
            t._output_replace(self, other)
        # TODO: right?
        other._in_tasks = self._in_tasks
        # This channel should no longer be used

        #TODO: negotiation
        self._invalidate()

    def _invalidate(self):
        self._state = CH_ERROR
        self._future = None
        self._bound = None


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
        self._in_tasks.append(input_task)

    def _register_output(self, output_task):
        """
        called when a task wants to read from this channel.
        Returns True if the channel is ready to be read from

        Default implementation simply adds to list of input
        tasks regardless.  Should be overridden with an
        implementation that checks that states are legal, etc.
        
        """
        self._out_tasks.append(output_task)
        #TODO: right?
        return self._state in [CH_OPEN_RW, CH_OPEN_R, CH_DONE_FILLED]

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

    def force(self, done_callback=None):
        """ 
        Forces evaluation of tasks to occur such that this channel will be filled 
        with data eventually.
        """
        global graph_mutex
        graph_mutex.acquire()
        try:
            self._force(done_callback=done_callback)
        finally:
            graph_mutex.release()


    def _force(self):
        """
        TODO: document
        Should be overridden.  The overriding method definition should:
        check states of inputs, update own state, start any inputs running
        """
        raise UnimplementedException("force not implemented on base Channel class")
    def _notify_done(self): 
        for cb in self._done_callbacks:
            cb(self)
        self._done_callbacks = []

    def state(self):
        """
        Return the current state of the channel
        """
        global graph_mutex
        graph_mutex.acquire()
        res = self._state
        graph_mutex.release()
        return res

    def readable(self):
        """
        Returns a boolean indicating whether the channel
        can be read from without blocking.

        This should be overridden as it depends on the type of channel.
        """
        raise UnimplementException("readable is not implemented")

    @classmethod
    def bind(cls, location):
        """
        Creates a new instance of the class, bound to some underlying data
        object or location.
        """
        return cls(_bind_location=location)
