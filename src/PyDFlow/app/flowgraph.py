'''
@author: Tim Armstrong
'''
from __future__ import with_statement

from PyDFlow.base.atomic import AtomicChannel, AtomicTask, Unbound
from PyDFlow.base.mutex import graph_mutex
from PyDFlow.base.exceptions import *
from PyDFlow.base.states import *
import LocalExecutor as localexec
from parse import parse_cmd_string

import atexit
import os
import tempfile
import shutil
import os.path
import logging
import errno

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
        

"""
Objects to represent a command line.
"""

def sub_arg(arg, output_paths):
    max_output = len(output_paths) - 1
    if isinstance(arg, Output):
        if arg.index > max_output:
            raise IndexError("Output index %d provided is out of range"
                             % arg.index)
        arg = output_paths[arg.index]
    # Force to string
    if arg is None:
        raise ValueError("None object provided to App")
    return str(arg)

class App(object):
    def __init__(self, *args, **kwargs):
        # Command format:
        # first argument is string with name of app
        # subsequent arguments are either strings passed to the
        # shell or a special AppFilePath object.
        self.command = args
        
        # Retrieve redirection info: variables
        # will be None if no redirection
        self.stdout = kwargs.pop('stdout', None)
        self.stdin = kwargs.pop('stdin', None)
        self.stderr = kwargs.pop('stderr', None)
        
        if len(kwargs) != 0:
            raise TypeError("Invalid keyword arguments provided by app task: %s " 
                            % ' '.join([key for key in kwargs]))
    
    def gen_command(self, output_paths):
        sub = lambda arg: sub_arg(arg, output_paths) 
        call_args = [sub(arg) for arg in self.command]
    
        if self.stdin is not None:
            self.stdin = sub(self.stdin)
        if self.stdout is not None:
            self.stdout = sub(self.stdout)
        if self.stderr is not None:
            self.stderr = sub(self.stderr)
        return (call_args, self.stdin, self.stdout, self.stderr)
         
        
class Output(object):
    def __init__(self, index):
        self.index = index
        
class OutputGen(object):
    def __getitem__(self, key):
        if isinstance(key, slice):
            # Return a list slice
            start = key.start
            if start is None:
                start = 0
            step = key.step
            if step is None:
                step = 1
            stop = key.stop
            if (stop - start) * step < 0:
                # step goes in wrong direction
                return []
            if step == 0:
                raise ValueError("slice step cannot be zero") 
            return [Output(i) for i in xrange(start, stop, step)]
                 
        else:
            return Output(key)
# user can index into this object to represent positional output args
outfiles = OutputGen()



class FileChannel(AtomicChannel):
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
        elif self._bound is Unbound:
            self._bound = self._mktmp()
            self._temp_created=True
        else:
            # make sure that directory exists so
            # task can correctly create file
            self._touch_file()
        
        

    def _touch_file(self):
        """
        Only works if channel bound.
        Will ensure that a file with the bound name can be
        created by the task when it runs
        """
        raise UnimplementedException("_touch_file() not implemented for base file channel")

    def _mktmp(self):
        """
        Creates an empty temporary file and returns path 
        to it.  Should not leave any file handles open
        """
        raise UnimplementedException("_mktmp not overridden")
    
    def _open_read(self):
        #TODO: exception types
        if not self._future.isSet():
            raise Exception("Reading from unset file channel")
        elif not self._fileExists(self._bound):
            raise Exception("Reading from nonexistent file")

    def _fileExists(self):
        raise UnimplementedException("_fileExists not overriden on file \
                channel object") 


    def _has_data(self):
        """
        Check to see if it is possible to start reading from this
        channel now
        """
        if self._bound is not Unbound:
            return self._fileExists(self._bound)
        else:
            return False

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
        if self._temp_created: 
            self._cleanup_tmp(self._bound)
            self._bound = None
            self._temp_created = False

    def copy(self, dest):
        with graph_mutex:
            if not self._bound:
                #TODO
                raise Exception("Cannot copy unbound FileChannel %s" %(repr(self))) 
            if self._state in (CH_OPEN_RW, CH_OPEN_R, CH_DONE_FILLED):
                self._docopy(self._bound, dest)
            else:   
                #TODO
                raise Exception("Cannot copy FileChannel %s, invalid state" % (repr(self)))

    def _docopy(self, src, dest):
        """
        Just copy the file..
        """
        raise UnimplementedException("__docopy was not overridden")

    def _cleanup_tmp(self, path):
        raise UnimplementedException("_cleanup_tmp was not overridden")
    
    def path(self):
        """
        Path returns the path to this file in the filesystem,
        or None if no path has been chosen.
        Default implemenation can be overridden
        """
        return self._bound


class LocalFileChannel(FileChannel):
    def __init__(self, *args, **kwargs):
        #TODO: expand bound path
        super(LocalFileChannel, self).__init__(*args, **kwargs)
        if self._bound is not Unbound:
            # Change to absolute path so that any subsequent changes
            # to OS working directory won't cause issues'
            self._bound = os.path.abspath(self._bound)

    def open(self):
        return open(self.get(), 'r')

    def _fileExists(self, file):
        return os.path.exists(file)
    
    def _touch_file(self):
        try:
            # recursive directory creation
            os.makedirs(os.path.dirname(self._bound))
        except OSError, e:
            # doesn't matter if already exists
            if e.errno == errno.EEXIST:
                pass
            else: 
                raise

    
    def _mktmp(self):
        """
        Creates an empty temporary file and binds self to it.
        """
        handle, path = tempfile.mkstemp()
        os.close(handle)
        tmpfiles.add(path) # track files
        return path

    def _cleanup_tmp(self, path):
        try:
            os.remove(path)
        except OSError:
            pass
        try:
            tmpfiles.remove(self._bound) # no longer track
        except KeyError:
            pass
    
    def _docopy(self, src, dest):
        # copy preserving metadata
        shutil.copy2(self._bound, dest)



class AppTask(AtomicTask):
    # By default, use local executor
    # TODO: need to dynamically choose executor based on:
    #   a) availability of required binary
    #   b) location of input files
    launch_app = localexec.launch_app

    def __init__(self, func, *args, **kwargs):
        super(AppTask, self).__init__(*args, **kwargs)
        self._func = func
        self._input_data = None
        self._in_exec_queue = False
        
    def _exec(self, continuation, failure_continuation,  contstack):
        
        # Lock while we gather up the input and output channels
        logging.debug("Gathering input values for %s" % repr(self))
        logging.debug("Inputs: %s" %(repr(self._inputs)))
        with graph_mutex:
            # Ensure only run once
            if self._in_exec_queue:
                # Another thread got here first
                logging.debug("App already being run - should this happen?")
                return
            else:
                self._in_exec_queue = True
            self._input_data = self._gather_input_values()
        logging.debug("Starting apptask - input values were %s" % repr(self._input_data))
        #logging.info("Queuing app task %s with inputs %s" % (self.name(), 
        #                            ' '.join([repr(arg) for arg in self._input_data])))
        # Launch the executable using backend module, attach a callback
        self.launch_app(continuation, failure_continuation, contstack)
    
    def isSynchronous(self):
        return False
    
    def _prepare_command(self):
        """
        Initializes i/o channels and collects data
        This presumes that graph_mutex is nt held prior to calling
        """
        with graph_mutex:
            self._prep_channels()
            out_paths =   [output._bound for output in self._outputs]
            
        app_object = self._func(*self._input_data)
        if not isinstance(app_object, App):
            raise TypeError("app function returned object that was not App")
        
          
        call_args, stdin_file, stdout_file, stderr_file = app_object.gen_command(out_paths)
        
        logging.debug("Generated command %s with redirections in=%s, out=%s, err=%s"
                      % (repr(call_args), stdin_file, stdout_file, stderr_file))
        return call_args, stdin_file, stdout_file, stderr_file

    def started_callback(self):
        # TODO: correct?
        # Don't acquire lock: the timing of this state
        # change is not critical
        self._state = T_RUNNING


