from PyDFlow.types.check import InputSpec, TaskDescriptor
import inspect
from functools import wraps



class task_decorator(object):
    """
    A generic task decorator that implements typing of functions.  

    In general, this decorator, upon decoratinga  function, needs to
    produce a callable object which, when called with proper arguments,
    creates a PyDFlow Task and its output channels, and returns the output
    channels.

    The decorator will add logic to validate the inputs of the  function
    against a provided type signature.
    and will tag the output of the function with the type

    E.g. 
    @somedec((Int), (Int, Int))
    def add (a, b):
        ....

    Customisation of behaviour by child classes is done by setting fields
    of this class.
    
    .task_wrapper is the class that will be returned as a replacement
        for the wrapper function.  The first four arguments to the constructor
        should be the function being decorated, the task class, the output types
        and the input specification.  Any additional arguments to the
        decorator are passed through to the wrapper class.
    .task_class is the actual task class, this is passed along to the wrapper
    """
    def __init__(self, output_types, input_types, *args, **kwargs):
        # args and kwargs are to be passed to wrapper class
        
        #Both inputs should be tuples or lists
        try:
            self.input_types = list(input_types)
        except TypeError:
            self.input_types = [input_types]
        try:
            self.output_types = list(output_types)
        except TypeError:
            self.output_types = [output_types]
        #print "Inputs:", self.input_types
        #print "Outputs:", self.output_types
        self.task_class = None
        self.wrapper_class = TaskWrapper
        self.args = args
        self.kwargs = kwargs

    def __call__(self, function):
        """
        Wraps the function
        """
        if self.task_class is None:
            raise Exception("task_class must be defined for task_decorator")

        descriptor = TaskDescriptor(function, self.input_types, 
                self.output_types) 

        wrapped = self.wrapper_class(function, self.task_class, descriptor, *(self.args), **(self.kwargs))
        # fix the name and docstring of the wrapped function.
        return wraps(function)(wrapped)


class TaskWrapper:
    def __init__(self, func, task_class, descriptor):
        self.func = func
        self.descriptor = descriptor
        self.task_class = task_class
        self._taskname = func.__name__

    def __call__(self, *args, **kwargs):
        # Set up the input/output channels and the tasks, plugging
        # them all together and validating types

        kwargs['_taskname']=self._taskname

        task = self.task_class(self.func, self.descriptor,
                                *args, **kwargs)
        # Unpack the tuple if necessary
        if len(task._outputs) == 1:
            return task._outputs[0]
        else:
            return task._outputs

    def __repr__(self):
        return "<PyDFlow Function: %s>" % repr(self._taskname)

