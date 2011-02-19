from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.flowgraph import graph_mutex
from PyDFlow.base.exceptions import *
from PyDFlow.base.states import *
import LocalExecutor as localexec
from parse import parse_cmd_string

import atexit
import os
import tempfile
import shutil
from os.path import exists
import logging


# Set of temp files not yet cleaned up
# protect with global_mutex
tmpfiles = set()

def __cleanup_alltmp():
    """
    To be called when python shuts down: garbage collection should ensure
    that most temp files are deleted but we can't guarantee this.
    """
    global tmpfiles
    for tfile in tmpfiles:
        logging.debug("Cleaning up temporary file %s" % tfile)
        os.remove(tfile)
    tmpfiles = set()

atexit.register(__cleanup_alltmp)
        

class FileChannel(AtomicChannel):
#TODO: delete temp files upon garbage collection.
    """
    A generic channel to handle all sorts of file data.

    File channels can be bound to a location in the file system,
    typically specified by a path string (although the exact
    details depend on the subclass).
    """
    def __init__(self, *args, **kwargs):
        super(FileChannel, self).__init__(*args, **kwargs)
        self._temp_created=False

    def open(self):
        """
        Returns a File-like object to read the file contents.
        This will block if the file's data is not yet available.
        """
        raise UnimplementedException("open() not implemented for \
                                        generic file channels")
    def _open_write(self):
        """
        In addition to the usual setup, if we are writing to a file, we
        need to choose a path for the file
        """
        if self._future.isSet(): 
            #TODO: exception
            raise UnimplementedException("Cannot write, future already set")
        elif self._bound is None:
            self._bind_tmp()
        else:
            #is bound, should be fine :)
            pass

    def _bind_tmp(self):
        """
        Creates an empty temporary file and binds self to it.
        Responsible for setting temp_created if temp needs tobe
        cleaned up
        """
        raise UnimplementedException("_create_tmp not overridden")
    
    def _open_read(self):
        #TODO: exception types
        if not self._future.isSet():
            raise Exception("Reading from unset file channel")
        elif not self._fileExists():
            raise Exception("Reading from nonexistent file")

    def _fileExists(self):
       raise UnimplementedException("_fileExists not overriden on file \
                channel object") 


    def _has_data(self):
        """
        Check to see if it is possible to start reading from this
        channel now
        """
        return self._fileExists()

    def __del__(self):
        """
        When garbage collection happens, clean up temporary files.
        
        Ie. tie the lifetime of the backing storage to this object's
        lifetime.
        Note: we will assume that references to the temporary file's path 
        won't escape this object.
        """
        # Don't lock, as GC was called can assume that no references held
        logging.debug("__del__ called on File channel")
        print "__del__ called"
        if self._temp_created: 
             self._cleanup_tmp()

    def copy(self, dest):
        global graph_mutex
        graph_mutex.acquire()
        self._copy(dest)
        graph_mutex.release()

    def _copy(self, dest):
        raise UnimplementedException("_copy was not overridden")

    def _cleanup_tmp(self):
        raise UnimplementedException("_cleanup_tmp was not overridden")

    def _write_done(self):
        logging.debug("Write done on %s" % repr(self))
        self._set(self._bound)

class LocalFileChannel(FileChannel):
    def __init__(self, *args, **kwargs):
        super(LocalFileChannel, self).__init__(*args, **kwargs)

    def open(self):
        return open(self.get(), 'r')

    def _fileExists(self):
        if self._bound is not None:
            return exists(self._bound)
        else:
            return False
    
    def _bind_tmp(self):
        """
        Creates an empty temporary file and binds self to it.
        """
        handle, path = tempfile.mkstemp()
        os.close(handle)
        self._bound = path
        tmpfiles.add(path) # track files
        self._temp_created=True

    def _cleanup_tmp(self):
        os.remove(self._bound)
        tmpfiles.remove(self._bound) # no longer track
        self._bound = None
        self._temp_created=False
    
    def _copy(self, dest):
        # copy preserving metadata
        if not self._bound:
            #TODO
            raise Exception("Cannot copy unbound FileChannel %s" %(repr(self))) 
        if self._state in [CH_OPEN_RW, CH_OPEN_R, CH_DONE_FILLED]:
            shutil.copy2(self._bound, dest)
        else:   
            #TODO
            raise Exception("Cannot copy FileChannel %s, invalid state" % (repr(self)))


    def path(self):
        """
        Path returns the path to this file in the filesystem,
        or None if no path has been chosen.
        """
        #TODO: push up to superclass?
        return self._bound


class AppTask(AtomicTask):
    def __init__(self, func, *args, **kwargs):
        super(AppTask, self).__init__(*args, **kwargs)
        self._func = func
        self._input_data = None
    
    def _exec(self):
        # Lock while we gather up the input and output channels

        #TODO: note, can the function really mess things up here 
        # by calling "get()"
        # or something similar on one of the input channels
        logging.debug("Gathering input values for %s" % repr(self))
        logging.debug("Inputs: %s" %(repr(self._inputs)))
        self._input_data = self._gather_input_values()
        logging.debug("Input values were %s" % repr(self._input_data))
        

        # TODO: set intermediate states - RUNNING, etc
        self._state = T_QUEUED
        
        logging.debug("Starting an AppTask")
        # Launch the executable using backend module, attach a callback
        localexec.launch_app(self)
    
    def _prepare_command(self):
        """
        Initializes i/o channels and collects data
        This presumes that graph_mutex is nt held prior to calling
        """
        graph_mutex.acquire()
        self._prep_channels()
        # Create a dictionary of variable names to file paths
        #TODO: what if input and output names overlap?
        path_dict = {}
        multi = []
        multi_count = 0
        multi_name = None
        for spec, input_path in self._descriptor.zip(self._input_data):
            if not spec.isRaw():
                if spec.isMulti():
                    if spec.fltype.internal.issubclassof(FileChannel):
                        path_dict["%s_%d" % (spec.name, multi_count)] = \
                                input_path
                        multi.append(input_path)
                        multi_name = spec.name
                        multi_count += 1
                elif spec.fltype.issubclassof(FileChannel):
                    path_dict[spec.name] = input_path
        if multi_name is not None:
            path_dict[multi_name] = multi

        for i, output in enumerate(self._outputs):
            path_dict["output_%d" % i] =  output._bound

        logging.debug("% s path_dict: %s" % (repr(self), repr(path_dict)))
        
        cmd_string = self._func(*self._input_data)
        #TODO: function should be able to also return a dictionary
        # no longer touching graph
        graph_mutex.release()
        
        # parse cmd_string, inject filenames, come up with a list of
        # arguments for the task
        call_args = parse_cmd_string(cmd_string, path_dict)
        #TODO: add in redirection
        stdin_file, stdout_file, stderr_file = (None, None, None)
        return call_args, stdin_file, stdout_file, stderr_file

    def started_callback(self):
        global graph_mutex
        # TODO: correct?
        # Don't acquire lock: the timing of this state
        # change is not critical
        self._state = T_RUNNING

    def finished_callback(self, popen):
        # Set all the output channels, trusting executable to have
        # done the right thing
        # TODO: check that files were created?
        logging.debug("%s finished" % repr(self))
        global graph_mutex
        graph_mutex.acquire()
        #TODO: check exit status?
        for c in self._outputs:
            c._write_done()
        self.state = T_DONE_SUCCESS
        graph_mutex.release()


