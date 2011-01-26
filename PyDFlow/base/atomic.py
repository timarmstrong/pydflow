from flowgraph import Task, Channel
from states import *
from PyDFlow.futures.futures import Future
from flowgraph import acquire_global_mutex, release_global_mutex

import logging


class AtomicTask(Task):
    """
    An atomic task can be bound to a future variable.
    The state of the channel is synchronized with the state of
    the future variable.  That is, the channel can only be read from if
    the variable is unset and only written to if the variable is set
    """
    def __init__(self, *args, **kwargs):
        super(AtomicTask, self).__init__(*args, **kwargs)
        if self._all_inputs_ready():
            self._state = T_DATA_READY

    def _input_readable(self, input, oldstate, newstate):
        """
        Overridden, we make sure that the task state is appropriately
        updated.
        Assume global lock is already held
        """
        super(AtomicTask, self)._input_readable(input, oldstate, newstate)
        if self._inputs_notready_count == 0:
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



class AtomicChannel(Channel):
    def __init__(self, *args, **kwargs):
        super(AtomicChannel, self).__init__(*args, **kwargs)
        # Create the future
        self._future = Future()
        # __future stores a handle to the underlying data 
        # future will be set exactly when the underlying data is
        # ready for reading: this way the get() function can block
        # on the future

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
        logging.debug("%s prepared" % (repr(self)))
        if mode == M_READWRITE:
            #TODO: exception type
            raise Exception("M_READWRITE is not valid for atomic channels")
        elif mode == M_WRITE: 
            #TODO: work out if it is bound to something, otherwise create a temp
            # State will be open for writing until something
            # is written into the file.
            if self._state == CH_CLOSED or self._state == CH_CLOSED_WAITING:
                self._open_write()
                self._state = CH_OPEN_W
            elif self._state == CH_OPEN_W or self._state == CH_OPEN_RW:
                pass
            else:
                #TODO: type
                raise Exception("Invalid state %d when trying to prepare for writing" %
                        self._state)
            #TODO: what if the channel is destroyed?
        elif mode == M_READ:
            if self._state == CH_OPEN_R or self._state == CH_OPEN_RW:
                pass
            elif self._state == CH_DONE_FILLED:
                self._open_read()
                self._state == CH_OPEN_R
            else:
                #TODO: exception type
                raise Exception("Read from channel which does not yet have data assoc")
        else:
            raise ValueError("Invalid mode to AtomicChannel._prepare %d" % mode)


    def _open_write(self):
        """
        Called when we want to prepare the channel for writing.  
        This does any required setup.  Not responsible for 
        state-related logic, but is responsible for ensuring
        that a write will proceed correctly.
        Override to implement alternative logic.
        """
        if self._future.isSet():
            #TODO: exception type
            raise Exception("Write to filled future channel")


    def _open_read(self):
        """
        Called when we want to prepare the channel for reading.  
        This does any required setup.  Not responsible for 
        state-related logic, but is responsible for ensuring
        that a read will proceed correctly.
        Override to implement alternative logic.
        """
        if not self._future.isSet():
             #TODO: exception type
            raise Exception("input channel has no data, cannot prepare")
        
    def _set(self, val):
        """
        Function to be called by input task when the data becomes available
        """
        oldstate = self._state
        if oldstate in [CH_OPEN_W, CH_OPEN_RW]:
            self._future.set(val)
            self._state = CH_DONE_FILLED
            #update the state and notify output tasks

            for t in self._out_tasks:
                t._input_readable(self, oldstate, CH_DONE_FILLED)
        else:
            #TODO: exception type
            raise Exception("Invalid state %d when atomic_channel set" % self._state)
            
        

    def get(self):
        acquire_global_mutex()
        try: 
            self._force()
        finally:
            release_global_mutex()
        return self._future.get() # block on future
    
    def _force(self):
        logging.debug("Atomic Channel forced")
        if self._state in [CH_CLOSED, CH_DONE_DESTROYED]:
            if self._bound is not None and self._in_tasks == []:
                if self._future.isSet():
                    # Data might be there, assume that binding was correct
                    self._state = CH_DONE_FILLED
                else: 
                    raise Exception("Forcing bound channel with no data and no input tasks")
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
            # In process of being filled, just wait
            pass
        elif self._state == CH_ERROR:
            #TODO: reraise exception
            # retry?
            raise Exception("Previous error: ")
        else:
            #TODO: exception type
            raise Exception("Invalid state code: %d" % self._state)

        def readable(self):
            """
            For atomic channels, it is readable either if it
            has been filled or it is bound.
            """
            return self._state in [CH_DONE_FILLED, CH_OPEN_R, CH_OPEN_RW] \
                or (self._state in [CH_CLOSED, CH_DONE_DESTROYED] \
                    and self._bound is not None and self._in_tasks == [])

