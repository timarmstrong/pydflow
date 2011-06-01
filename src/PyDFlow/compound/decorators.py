'''
@author: Tim Armstrong
'''

from compound import CompoundTask, IvarPlaceholder
from PyDFlow.base.decorators import task_decorator, TaskWrapper
import logging

class compound(task_decorator):
    def __init__(self, output_types, input_types):
        #output_types = [PlaceholderType(t) for t in output_types]
        super(compound, self).__init__(output_types, input_types)
        self.task_class = CompoundTask
        self.wrapper_class = CompoundWrapper
        

def sub_placeholder(ivar_class):
    def place_wrap():
        logging.debug("place_wrap invoked")
        return IvarPlaceholder(ivar_class)
    return place_wrap
 
        
class CompoundWrapper(TaskWrapper):
    def __init__(self, func, task_class, descriptor):
        descriptor.set_output_wrapper(sub_placeholder)
        super(CompoundWrapper, self).__init__(func, task_class, descriptor)