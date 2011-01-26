from PyDFlow.base.exceptions import *



#======================OLD BELOW
# cython: profile=True

import logging
import base
import tempfile
import app_executor
from base import assign






class FileChannel(base.FutureChannel):
    """
    FileChannel implements communication between tasks using files in the
    local file system.  It is based on FutureChannel, putting the path of the
    file into the future variable.  If the future variable is set to a pathname,
    this means both that the file channel is using this file to communicate,
    and that the data is already present in this file.

    Tasks using this Channel as output will signal that they have completed
    by calling set with the filename.

    If self.file_path is set, this means that the FileChannel has been mapped
    to something in the filesystem
    """
    def __init__(self):
        base.FutureChannel.__init__(self)
        self.file_path = None

    def ensure_prepared(self):
        self.prepare()

    def prepare(self):
        """
        Three valid scenarios:
            * Not mapped to any file path, an input task -> use temp file
            * Mapped to a file, no input task -> use existing file data
            * Mapped to a file, an input task -> input task fills file
        """
        # Need to be locked someof the time and unlocked some of the time,
        # so we implement prepare rather than _prepare
        logging.debug("Entered prepare() in FileChannel")
        self.lock()
        if self._is_prepared():
            self.unlock()
            return

        self._prepare()
        if len(self.input_tasks) > 0:
            # Just need to make sure there is a destination for the file
            if self.file_path is None:
                self.file_path = tempfile.mktemp()
        else:
            # This ought to have been mapped to something
            if self.file_path is None:
                raise Exception("No path could be determined for FileChannel:"
                            + " either need to map to a file, or generate file"
                            + " as app output")
            else:
                self._set(self.file_path)
        self.unlock()
    
    def _set(self, data):
        if data != self.file_path:
            raise Exception("FileChannel should only be set to the predetermined "
                    + "file path")
        super(FileChannel, self)._set(data)

    @classmethod
    def map(cls, file_path):
        """
        Creates a new instance of the class, mapped to a file in the file system
        There are two ways this information may be used.  If this channel
        has no input task, then the data will be assumed to be present in this
        specified file.
        If the channel has an input task, then the file will be used as a 
        destination.
        """
        o = cls()
        o.file_path = file_path
        return o

