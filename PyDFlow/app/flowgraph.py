from PyDFlow.base.atomic import AtomicChannel, AtomicTask
from PyDFlow.base.exceptions import *


class FileChannel(AtomicChannel):
    """
    A generic channel to handle all sorts of file data
    """
    def __init__(self, *args, **kwargs):
        super(FileChannel, self).__init__(*args, **kwargs)

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
        raise UnimplementedException("_open_write  depends on where file is")

class LocalFileChannel(AtomicChannel):
    def __init__(self, *args, **kwargs):
        super(LocalFileChannel, self).__init__(*args, **kwargs)

    def open(self):
        return open(self.get(), 'r')

    def _open_write(self):
        """
        In addition to the usual setup, if we are writing to a file, we
        need to choose a path for the file
        """
        # Future should in fact be set, as we need to know where to write to???
        #TODO: this doesn't work?????
        if not self._future.isSet(): 
           self.file 

