'''
@author: Tim Armstrong
'''
from PyDFlow.base.decorators import task_decorator, TaskWrapper
from PyDFlow.base.atomic import AtomicChannel, AtomicTask

import pycuda.autoinit
import pycuda.driver as drv
import numpy as np

from pycuda.compiler import SourceModule




class cuda_kernel(task_decorator):
    """
    TODO: for now assume that function is a zero argument function.
    """
    def __init__(self, func_name, output_types, input_types):
        super(cuda_kernel, self).__init__(output_types, input_types)
        # Stick the func_name in the tuple, so that the function
        # can be pulled out of the module text
        self.func = (func_name, self.func)
        self.wrapper_class = _CUDAKernel
        self.task_class = CUDATask
        

class CUDATask(AtomicTask):
    """
    TODO: Intelligence to work out what stream this belongs to
        Channels view: if all input channels belong to same stream,
            then this channel has a stream.  The first output channel
            can then be assigned to this stream too.  Other output channels
            will have to wait until the async channel has actually finished.

    """
    def __init__(self, block, grid, *args,  **kwargs):
        super(CUDATask, self).__init__(self, *args, **kwargs)
        self.block = block
        self.grid = grid
        #self.block = kwargs.get("block", None)
        #self.grid = kwargs.get("grid", (1,1))


    def _exec(self, continuation, failure_continuation, contstack=None):
        # TODO: update state
        # TODO: prep channels.  If GPU channels are already on device, we're good.  Otherwise
        #    we will need to worry about starting asynchronously a data transfer
        
        # TODO: set input arguments
        
        # TODO: launch asynchronous kernel by sending to GPU host thread via queue
        pass
        
    def isSynchronous(self):
        return False

class _CUDAKernel(TaskWrapper):
    """
    Similar to TaskWrapper - this can either be invoked directly
    by the user, or can be invoked indirectly with the @cuda_kernel decorator
    """
    def __init__(self, func_name_source_mod_tup, task_class, descriptor, default_block=(1,1), default_grid=(1,1)):
        func_name, source_mod = func_name_source_mod_tup
        # Compile the function
        self.cuda_func = source_mod.get_function(func_name)
        self._descriptor = descriptor #TODO: right?
        self.default_block = default_block
        self.default_grid = default_grid

    def __call__(self, *args, **kwargs):
        """
        Extra logic to fill in default blocks/grids
        """
        block = kwargs.get("block", self.default_block)
        if not block:
            raise ValueError("No block parameter provided, and no default block size for this CUDA function")
        else:
            kwargs['block'] = block
        grid = kwargs.get("grid", self.default_grid)
        if grid:
            kwargs['grid'] = grid
        #Delegate to parent class
        super(_CUDAKernel, self).__call__(self, *args, **kwargs)

class CUDAChannel(AtomicChannel):
    """
    This class represents a numpy array that can be moved between CPU and
    GPU, and passed between GPU kernels.

    This class be in several states:

    filled -> this class has a reference to the data
    filling synchronously -> predecessor tasks have been set to run.  When the input tasks complete, then
        this runtime will ensure that this channel is filled
    filling asynchronously -> all the predecessor tasks/channels are being executed/filled asynchronously by
        the CUDA runtime.  TODO: mechanism to update states as required.
    not started -> ... self explanatory

    There are several states the data can be stored when it is filled
        -> After being mapped to a numpy array in main memory, only holds a reference to this
        -> after being filled by a compute kernel on the gpu, only holds a reference to the array on the GPU
        -> holds a reference to identical CPU and GPU arrays.
    The data is staged from GPU to CPU when get() is called
    The data is staged from CPU to GPU when prepare() is called
    When fill is called, all the work to fill in the GPU array is started up

    """
    pass


cudavar = CUDAChannel

class CUDAKernel(_CUDAKernel):
    """
    Hack to separate out func_name and source_mod - nicer interface for users
    of module
    """
    def __init__(self, func_name, source_mod, *args, **kwargs):
        _CUDAKernel.__init__(self, (func_name, source_mod), *args, **kwargs)
    
