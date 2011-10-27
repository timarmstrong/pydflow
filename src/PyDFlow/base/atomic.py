# Copyright 2010-2011 Tim Armstrong <tga@uchicago.edu>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
@author: Tim Armstrong
'''
from __future__ import with_statement
from flowgraph import Task, Ivar, Unbound
from states import *
from PyDFlow.writeonce import WriteOnceVar
from PyDFlow.base.exceptions import *
from PyDFlow.base.mutex import graph_mutex


import logging

import LocalExecutor
from PyDFlow.base.flowgraph import ErrorVal


class AtomicTask(Task):
    """
    An atomic task can be bound to a future variable.
    The state of the Ivar is synchronized with the state of
    the future variable.  That is, the Ivar can only be read from if
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
                #TODO: valid transition?
                self._state = T_DATA_READY
            logging.debug("%s changed state" % repr(self))

    def _gather_input_values(self):
        """
        All the input values should eb ready when this function is
        called.  This will mean that handles to all the input data
        can be passed to the actual task, which is then capable of
        directly accessing them.
        """
        input_data = []
        for spec, inp in self._input_iter():
            if spec.isRaw():
                input_data.append(inp)
            else:
                input_data.append(inp._get())

        return input_data

    def _prep_ivars(self):
        for s, o in self._input_iter():
            if not s.isRaw():
                o._prepare(M_READ)
        # Ensure outputs can be written to
        for o in self._outputs:
            o._prepare(M_WRITE)

        
        

class AtomicIvar(Ivar):
    def __init__(self, *args, **kwargs):
        super(AtomicIvar, self).__init__(*args, **kwargs)

        
        # Create the future
        self._future = WriteOnceVar()
        # __future stores a handle to the underlying data 
        # future will be set exactly when the underlying data is
        # ready for reading: this way the get() function can block
        # on the future

        # Whether the backing storage is reliable
        self._reliable = True


    def _register_input(self, input_task):
        """ 
        Ivar can only be written to once, by a single writer.
        """
        #TODO: proper exception types
        if len(self._in_tasks) > 0:
            raise Exception("Multiple tasks writing to an AtomicIvar")
        elif self._state != IVAR_CLOSED:
            raise Exception("Adding an input to an open AtomicIvar")
        else:
            self._in_tasks.append(input_task)

    def _register_output(self, output_task):
        """
        Don't need to track output task for notifications if ivar is reliable.
        """
        done = self._state in [IVAR_OPEN_RW, IVAR_OPEN_R, IVAR_DONE_FILLED]
        if done and self._reliable:
            return True
        else:
            self._out_tasks.append(output_task)
            #TODO: right?
            return done

    def _replacewith(self, other):
        # Merge the futures to handle the case where a thread
        # is block on the current thread's future
       # other._future.merge_future(._future)
        super(AtomicIvar, self)._replacewith(other)


    def _prepare(self, mode):
        """
        Set up the future variable to be written into.
        """
        logging.debug("%s prepared" % (repr(self)))
        if mode == M_READWRITE:
            #TODO: exception type
            raise Exception("M_READWRITE is not valid for atomic Ivars")
        elif mode == M_WRITE: 
            #TODO: work out if it is bound to something, otherwise create a temp
            # State will be open for writing until something
            # is written into the file.
            if self._state == IVAR_CLOSED or self._state == IVAR_CLOSED_WAITING:
                self._open_write()
                self._state = IVAR_OPEN_W
            elif self._state == IVAR_OPEN_W or self._state == IVAR_OPEN_RW:
                pass
            else:
                #TODO: type
                raise Exception("Invalid state %d when trying to prepare for writing" %
                        self._state)
            #TODO: what if the Ivar is destroyed?
        elif mode == M_READ:
            if self._state == IVAR_OPEN_R or self._state == IVAR_OPEN_RW:
                pass
            elif self._state == IVAR_DONE_FILLED:
                self._open_read()
                self._state = IVAR_OPEN_R
            else:
                #TODO: exception type
                raise Exception("Read from Ivar which does not yet have data assoc")
        else:
            raise ValueError("Invalid mode to AtomicIvar._prepare %d" % mode)


    def _open_write(self):
        """
        Called when we want to prepare the Ivar for writing.  
        This does any required setup.  Not responsible for 
        state-related logic, but is responsible for ensuring
        that a write will proceed correctly.
        Override to implement alternative logic.
        """
        if self._future.isSet():
            #TODO: exception type
            raise Exception("Write to filled future Ivar")


    def _open_read(self):
        """
        Called when we want to prepare the Ivar for reading.  
        This does any required setup.  Not responsible for 
        state-related logic, but is responsible for ensuring
        that a read will proceed correctly.
        Override to implement alternative logic.
        """
        if not self._future.isSet():
             #TODO: exception type
            raise Exception("input Ivar has no data, cannot prepare")
        
    def _set(self, val):
        """
        Function to be called by input task when the data becomes available
        """
        oldstate = self._state
        if oldstate in (IVAR_OPEN_W, IVAR_OPEN_RW):
            self._future.set(val)
            
            self._state = IVAR_DONE_FILLED
            logging.debug("%s set" % repr(self))
            #update the state and notify output tasks
            for t in self._out_tasks:
                t._input_readable(self, oldstate, IVAR_DONE_FILLED)

            if self._reliable:
                self._in_tasks = None
                self._out_tasks = None # Don't need to provide notification of any changes'
            self._notify_done()
        else:
            #TODO: exception type
            raise Exception("Invalid state when atomic_Ivar %s set" % repr(self))
            
        

    def get(self):
        with graph_mutex:
            res = self._get() # block on future
        return res  

    def _get(self):
        """
        For internal use only: get directly from local future, don't
        bother forcing or locking or anything.
        Should have graph lock first.
        Only call when you are sure the Ivar has data ready for you.
        """
        if LocalExecutor.isWorkerThread():
            if  not self._state in (IVAR_OPEN_R, IVAR_OPEN_RW, IVAR_DONE_FILLED):
                # This thread goes off and runs stuff recursively
                # before blocking
                LocalExecutor.spark_recursive(self)    
        else:
            self._spark()
        graph_mutex.release()
        try:
            res = self._future.get()
        finally:
            graph_mutex.acquire()
        if res is ErrorVal and self._state == IVAR_ERROR:
            raise self._exception
        return res

    def _error(self, *args, **kwargs):
        self._future.set(None)
        super(AtomicIvar, self)._error(*args, **kwargs)
        
    def _has_data(self):
        raise UnimplementedException("_has_data not overridden")

    def _try_readable(self):
        logging.debug("_try_readable on %s" % repr(self))
        if self._state in (IVAR_DONE_FILLED, IVAR_OPEN_R, IVAR_OPEN_RW):
            return True
        elif self._state in (IVAR_CLOSED, IVAR_DONE_DESTROYED):
            if len(self._in_tasks) == 0:
                if self._bound is Unbound:
                    raise NoDataException("Unbound Ivar with no input tasks was forced.")
                else:
                    if self._has_data():
                        # Data might be there, assume that binding was correct
                        self._prepare(M_WRITE)
                        self._set(self._bound)
                        return True
                    else: 
                        raise NoDataException(("Bound Ivar with no input tasks " + 
                                              "and no associated data was forced. " +
                                             "Ivar was bound to" + repr(self._bound)))
                        
        elif self._state in (IVAR_ERROR,):
            return False
        else:
            return False
                    


    def _spark(self, done_callback=None):
        """
        Should be called with lock held
        Ensure that at some point in the future this Ivar will be filled
        """
        logging.debug("Atomic Ivar sparked")
        if done_callback is not None:
            self._done_callbacks.append(done_callback)

        if self._state in (IVAR_CLOSED, IVAR_DONE_DESTROYED):
            if self._bound is not Unbound and len(self._in_tasks) == 0:
                try:
                    self._try_readable()
                except NoDataException, ex:
                    # Don't need to propagate error: this method only run if this
                    # Ivar is forced manually
                    self._fail([ex])
                    self._notify_done()
                
            elif len(self._in_tasks) > 0:
                # Enable task to be run, but
                # input tasks should be run first
                self._state = IVAR_CLOSED_WAITING
                LocalExecutor.exec_async(self)
            else:
                # Nowhere for data to come from
                #TODO: exception type
                raise Exception("forcing Ivar which has no input tasks or bound data")
        elif self._state in (IVAR_CLOSED_WAITING, IVAR_OPEN_W):
            # Already sparked, just wait
            pass
        elif self._state in (IVAR_OPEN_R, IVAR_OPEN_RW, IVAR_DONE_FILLED):
            # Filled: notify all, including provided callback
            self._notify_done()
        elif self._state == IVAR_ERROR:
            # It is upto get to check.
            self._notify_done()
            raise self._exception
        else:
            #TODO: exception type
            raise Exception("Invalid state code: %d" % self._state)

    def readable(self):
        with graph_mutex:
            return self._readable()
        
    def _readable(self):
        """
        For atomic Ivars, it is readable either if it
        has been filled or it is bound.
        """
        return self._state in [IVAR_DONE_FILLED, IVAR_OPEN_R, IVAR_OPEN_RW] \
            or (self._state in [IVAR_CLOSED, IVAR_DONE_DESTROYED] \
                and self._bound is not None and len(self._in_tasks) == 0)

