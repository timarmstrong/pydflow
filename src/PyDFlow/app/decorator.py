'''
@author: Tim Armstrong
'''
from PyDFlow.base.decorators import task_decorator
from flowgraph import AppTask

class app(task_decorator):
    """
    The app decorator.  It is applied to a python function
    which generates a command string to be executed.

    E.g.
    @app((Image), (Image, _))
    """
    def __init__(self, output_types, input_types):
        super(app, self).__init__(output_types, input_types)
        self.task_class = AppTask
