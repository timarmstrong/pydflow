from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.exceptions import *
from os.path import exists
import os
import tempfile
import shutil


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

    def openFile(self):
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
        if not self._future.isSet() 
            raise Exception("Reading from unset file channel")
        elif not self._fileExists():
            raise Exception("Reading from nonexistent file")

    def _fileExists(self):
       raise UnimplementedException("_fileExists not overriden on file \
                channel object") 

    def __del__(self):
        """
        When garbage collection happens, clean up temporary files.

        Note: we can assume that 
        """
        # Don't lock, as GC was called can assume that no references held
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

class LocalFileChannel(AtomicChannel):
    def __init__(self, *args, **kwargs):
        super(LocalFileChannel, self).__init__(*args, **kwargs)

    def open(self):
        return open(self.get(), 'r')

    def _fileExists(self):
        if self._future.isSet():
            return exists(self._bound)
        else:
            return False
    
    def _bind_tmp(self):
        """
        Creates an empty temporary file and binds self to it.
        """
        handle, self._bound = tempfile.mkstemp()
        os.close(handle)
        self._temp_created=True

    def _cleanup_tmp(self):
        os.remove(self._bound)
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
