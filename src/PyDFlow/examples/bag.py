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

#!/usr/bin/python
'''
@author: Tim Armstrong
'''
import random
import time
import logging
from PyDFlow import *
from PyDFlow.PyFun import *
#logging.basicConfig(level=logging.DEBUG)

Int = py_ivar.subtype()


@func((Int), (Int))
def rand_dur(x):
    """
    Sleep for a random duration and then return the argument.
    """
    dur = random.random() * 1
    time.sleep(dur)
    return x

inp = [Int(i) for i in range(20)]

out = map(rand_dur, inp)

#b = resultset(out, max_running=2)
b = resultset(out)

def use_rbag(bag):    
    res = []
    for i, c in bag:
        print i, c.get()
        res.append((i, c.get()))

    print len(res), "results"

print "Testing result collection with fresh ivars"
use_rbag(b)
print "Testing result collection with ready ivars"
use_rbag(b) # check that it works ok with already finished tasks

inp2 = [Int(i) for i in range(20)]

out2 = map(rand_dur, inp)

b = resultset(out2 + out )
print "Testing result collection with mix of ivars"
use_rbag(b) # check that it works ok with already finished tasks
