from PyDFlow.base.decorators import task_decorator
from flowgraph import FuncTask, CompoundTask

class func(task_decorator):
    """
    The func decorator.  When applied to a python function, it can be
    the function can be used as a "task".

    The func decorator will validate the input types of the  function,
    and will tag the output of the function with the type

    E.g. 
    @func((Int), (Int, Int))
    def add (a, b):
        return a + b
    """
    def __init__(self, output_types, input_types):
        super(func, self).__init__(output_types, input_types)
        self.task_class = FuncTask

class compound(task_decorator):
    def __init__(self, output_types, input_types):
        super(compound, self).__init__(output_types, input_types)
        self.task_class = CompoundTask