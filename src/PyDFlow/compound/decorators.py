# Copyright 2010-2011 Tim Armstrong <tga@uchicago.edu>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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