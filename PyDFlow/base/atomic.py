from flowgraph import Task, Channel
from states import *
from PyDFlow.futures.futures import Future
from flowgraph import acquire_global_mutex, release_global_mutex

import logging


class AtomicTask(Task):
    """
    An atomic task can be bound to a future variable.
    """
    def __init__(self, *args, **kwargs):
        super(AtomicTask, self).__init__(*args, **kwargs)
        self.__future = None
        if self._all_inputs_ready():
            self.state = T_DATA_READY

    def _input_readable(self, oldstate, newstate):
        """
        Overridden, we make sure that the task state is appropriately
        updated.
        Assume global lock is already held
        """
        super(AtomicTask, self)._input_readable(self, input, oldstate, newstate)
        if self._all_inputs_ready():
            if self._state == T_INACTIVE:
                self._state = T_DATA_READY
            elif self._state == T_DATA_WAIT:
                self._startme()

    def _gather_input_values(self):
        """
        All the input values should eb ready when this function is
        called.  This will mean that handles to all the input data
        can be passed to the actual task, which is then capable of
        directly accessing them.
        """
        input_data = []
        for inp, spec in zip(self._inputs, self._input_spec):
            if spec.isRaw():
                input_data.append(inp)
            else:
                input_data.append(inp.get())
        return input_data
    
    def _input_readable(self, input, oldstate, newstate):
        """
        """
        super(AtomicTask, self)._input_readable(self, input, oldstate, newstate)
        if self._inputs_notready_count == 0:
            if self._state == T_INACTIVE:
                self._state = T_DATA_READY
            elif self._state == T_DATA_WAIT:
                self._startme()



class AtomicChannel(Channel):
    def __init__(self, *args, **kwargs):
        super(AtomicChannel, self).__init__(*args, **kwargs)

        # __future stores a handle to the data
        self._future = None

    def _register_input(self, input_task):
        """ 
        Channel can only be written to once, by a single writer.
        """
        #TODO: proper exception types
        if self._in_tasks != []:
            raise Exception("Multiple tasks writing to an AtomicChannel")
        elif self._state != CH_CLOSED:
            raise Exception("Adding an input to an open AtomicChannel")
        else:
            self._in_tasks.append(input_task)

    def _prepare(self, mode):
        """
        Set up the future variable to be written into.
        """
        if mode == M_READWRITE:
            #TODO: exception type
            raise Exception("M_READWRITE is not valid for atomic channels")
        elif mode == M_WRITE: 
            #TODO: work out if it is bound to something, otherwise create a temp
            # State will be open for writing until something
            # is written into the file.
            if self._state == CH_CLOSED or self._state == CH_CLOSED_WAITING:
                # create a future if needed
                if self._bound:
                    self._open_bound_write()
                else:
                    self._open_bound()
                self._state = CH_OPEN_W
            elif self._state == CH_OPEN_W or self._state == CH_OPEN_RW:
                pass
            else:
                #TODO: type
                raise Exception("Invalid state %d when trying to prepare for writing" %
                        self._state)
            #TODO: what if the channel is destroyed?
        elif mode == M_READ:
            if self._state == CH_CLOSED:
                if self._bound: 
                    # Open, using bound location
                    self._open_bound_read()
                    self._state = CH_OPEN_R
                else:
                    # No data in channel and nothing is being written here
                    #TODO: exception type
                    raise Exception("Want to read from channel, but no data associated")
            elif self._state == CH_OPEN_R or self._state == CH_OPEN_RW:
                pass
            elif self._state == CH_DONE_FILLED:
                self._open_read()
                self._state == CH_OPEN_R
            else:
                #TODO: exception type
                raise Exception("Read from channel which does not yet have data assoc")
        else:
            raise ValueError("Invalid mode to AtomicChannel._prepare %d" % mode)

    def _open_bound_write(self):
        """
        Called when we want to prepare the channel for writing, and it is bound
        to something.  This does any required setup.  not responsible for 
        setting state.
        Override to implement alternative logic.
        """
        self._future = self._bound
        if self.bound.isSet():
            #TODO: exception type
            raise Exception("Bound to future that is already filled")

    def _open_write(self):
        """
        Called when we want to prepare the channel for writing, and it is not
        bound.  This does any required setup.  not responsible for 
        setting state.
        Override to implement alternative logic.
        """
        self._future = Future()

    def _open_bound_read(self):
        """
        Called when we want to prepare the channel for reading, and it is bound
        to something.  This does any required setup.  not responsible for 
        setting state.
        Override to implement alternative logic.
        """
        self._future = self._bound
        if not self._bound.isSet():
             #TODO: exception type
            raise Exception("bound unset future as input channel, cannot preprate")
    def _open_read(self):
        """
        Called when we want to prepare the channel for reading, and it is not
        bound.  This does any required setup.  not responsible for 
        setting state.
        Override to implement alternative logic.
        """
        pass
        

    def get(self):
        self.force()
        return self._future.get() # block on future
    
    def _force(self):
        logging.debug("Atomic Channel forced")
        if self._state in [CH_CLOSED, CH_DONE_DESTROYED]:
            if self._bound and self._in_tasks == []:
                # Data might be there
                self._prepare(M_READ)
            elif self._in_tasks != []:
                # Enable task to be run, but
                # input tasks should be run first
                self._state = CH_CLOSED_WAITING
                for f in self._in_tasks:
                    f._force()
            else:
                # Nowhere for data to come from
                #TODO: exception type
                raise Exception("forcing channel which has no input tasks or bound data")
        elif self._state in [CH_CLOSED_WAITING, CH_OPEN_R, CH_OPEN_RW]:
            # Already forced, just wait
            pass
        elif self._state in [CH_OPEN_W, CH_DONE_FILLED]:
            self._prepare(M_READ)
        elif self._state == CH_ERROR:
            #TODO: reraise exception
            # retry?
            raise Exception("Previous error: ")
        else:
            #TODO: exception type
            raise Exception("Invalid state code: %d" % self._state)
