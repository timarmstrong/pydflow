

"""
We need to have a single Host Thread managing all the CUDA kernels/transfers.  
All other host threads must then launch CUDA kernels by putting them on the 
queue for this host thread.  The caller must provide a callback function to 
be notified when the kernel has finished execution.

All kernels will be launched asynchronously.  The caller can optionally provide
a stream - if they don't provide one, we will put in its own stream (we need
to use the stream interface to be notified when things complete)

Streams:
    If tasks all belong to the same stream, then it might impose unnecessary 
    overhead to track when each kernel finishes separately.
    We should wait until the entire stream completes

TODO: how do we handle mallocing.  The host thread must do the mallocing,
    and it is synchronous.
    Ideas:
    - Have this module receive references to a CUDATask, and trace back through
       the graph to figure out what must be done.
    - Pass CUDAIvar references into this class - the output ivar of a 
        given task, and have the host thread fill in a dev_ptr in the 
        CUDAIvar immediately before launching an asynchronous task
    - Some scheme where there is a separate mallocing queue

@author: Tim Armstrong
"""

class CUDAJob:
    """
    TODO: 
        * Copy memory d->h
        * Copy memory h->d
        * Run kernel on data
    """
    pass

class StreamHandle:
    def __init__(self, stream=None):
        self.stream = stream
    def fillin(self, stream):
        if self.stream:
            raise ValueError("Tried to set stream handle twice")
        self.stream = stream

def new_stream():
    """
    Return a new handle to a CUDA stream
    """
    return StreamHandle()

def do_memcpy_h_d_async(ivar, callback, stream=None):
    # TODO: add to queue of tasks to run
    pass



def do_memcpy_d_h_async(ivar, callback, stream=None):
    # TODO same recipe as above
    pass

def do_memcpy_d_h(ivar, stream=None):
    pass
    # TODO: set up a cond var

    # TODO: launch the async version, with a callback that signals cond
    #    var
    # TODO: block on cv

def do_kernel_async(kernel_func, output_ivars, callback, stream=None):
    pass
    # TODO: add to queue



def fill_stream_handle(stream):
    """
    To be run only be main host thread
    """
    if stream is None:
        pass
        #TODO: create a new stream for this
    elif stream.stream is None:
            # TODO: Create a real one and wrap it
    return stream
        
# Functions run by the host thread to actually kick off execution
def run_jobs(job_list, stream, callback):
    # TODO Run all the jobs in a single stream, and run the callback
    # once they're all done
    pass

def run_memcpy_h_d(ivar, callback, stream):
    #TODO: launch copy

    #TODO: setup something to watch the memcpy, and add it to a queue

    return stream

def run_kernel_async(kernel_func, output_ivars, callback, stream=None):
    #TODO: 
