from PyDFlow.base.exceptions import *



#======================OLD BELOW
# cython: profile=True

import logging
import base
import tempfile
import app_executor
from base import assign

class app(base.task_decorator):
    """
    The app decorator. 
    """
    def __init__(self, output_types, input_types):
        super(app, self).__init__(output_types, input_types)
        self.task_class = AppTask
        self.function_converter = unit

def unit(x):
    return x

class DoneCallback:
    def __init__(self, task):
        self.task = task
    def __call__(self, popen):
        self.task.finished_callback(popen)

class AppTask(base.AtomicTask):
    def __init__(self, converted_function, output_types, input_spec,
            *args, **kwargs):
        super(AppTask, self).__init__(output_types, 
                                input_spec, *args, **kwargs)
        self.function = converted_function
    
    def _run(self):
        logging.debug("_run() in AppTask")
        # Lock while we gather up the input and output channels

        #TODO: note, can the function really mess things up here by calling "get()"
        # or something similar on one of the input channels
        input_data = self._gather_input_data()

        # Create a dictionary of variable names to file paths
        #TODO: what if input and output names overlap?
        

        path_dict = dict(
                [(spec.name, input_path)
                    for input_path, spec 
                    in zip(input_data, self.input_spec)
                    if spec.swtype and FileChannel.issubclass(spec.swtype)]
              + [("output_%d" % i, output_chan.file_path)
                    for i, output_chan
                    in enumerate(self.output_channels)] )
        logging.debug("path_dict: " + repr(path_dict)) 
                
        
        cmd_string = self.function(*input_data)

        #parse cmd_string, inject filenames, come up with a list of
        # arguments for the task
        call_args, stdin, stdout, stderr = parse_cmd_string(cmd_string, path_dict)
        # TODO: set intermediate states - RUNNING, etc
        # Launch the executable using backend module, attach a callback
        app_executor.launch_app(call_args, DoneCallback(self), 
                    stdin, stdout, stderr)


    def finished_callback(self, popen):
        # Set all the output channels, trusting executable to have
        # done the right thing
        # TODO: check that files were created?
        for c in self.output_channels:
            c.set(c.file_path)
        #TODO: check exit status?
        self.state = base.STATE_FINISHED 

def process_token(tok, path_dict, substitute_path):
    tok = ''.join(tok) # concatenate list of strings
    if substitute_path:
        # Strip off "@" and look up path
        path = path_dict.get(tok[1:], None)
        if not path:
            raise Exception("No such argument to app as %s, cannot resolve %s, path_dict: %s" % (
                    tok[1:], tok, repr(path_dict)))
        return path
    else:
        return tok

def parse_cmd_string(cmd_string, path_dict):
    #TODO: stdin/stdout/stderr redirect
    
    #tokens: quote delimited strings, all other tokens are separated
    # by spaces. While in quote, \", \' and \\ are escape sequences
    in_quote = None
    tokens = []
    curr_tok = [] # array of characters
    in_token = False  # Can tell if in_token by curr_tok being "", except
                      # in the case where there is a zero-length quoted string
                      # so we keep a separate bool here for that case
    escaped = False
    substitute_path = False
    for c in cmd_string:
        if in_quote:
            if escaped:
                escaped = False
                curr_tok.append(c)
            elif c == in_quote:
                in_quote = None
            elif c == "\\": # escape char
                escaped = True
            else:
                curr_tok.append(c)
        else:
            # not in quote
            if c.isspace(): # end of current token
                if in_token: # if this was the end of a token
                    tokens.append(process_token(curr_tok, path_dict, substitute_path))
                    curr_tok = []
                    in_token = False
                    substitute_path = False
            elif c == "\'" or c == "\"":
                in_quote = c
                in_token = True
            else:
                in_token = True
                if c == "@":
                    substitute_path = True
                curr_tok.append(c)
    if in_quote:
        raise Exception("Unclosed quote %s in command string: %s" % (in_quote, cmd_string))
    if in_token:
        tokens.append(process_token(curr_tok, path_dict, substitute_path))

    logging.debug("Given pathname dict %s and cmdstring %s, result was %s" % (
            repr(path_dict), cmd_string, repr(tokens)))
    return tokens, None, None, None


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

swfile = FileChannel
