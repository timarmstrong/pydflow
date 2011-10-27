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

from PyDFlow.base.decorators import task_decorator
from flowgraph import FuncTask

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
