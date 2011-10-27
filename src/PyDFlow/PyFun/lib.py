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

from random import Random
from PyDFlow.PyFun import py_ivar
import PyDFlow.base.rand as base
from functools import wraps

def rlift(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return (py_ivar(r) for r in f(*args, **kwargs))


genrandom = rlift(base.genrandom)
genrandint = rlift(base.genrandom)
gensample = rlift(base.gensample)