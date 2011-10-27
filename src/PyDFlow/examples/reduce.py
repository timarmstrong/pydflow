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
from PyDFlow import *
from PyDFlow.PyFun import *
import random
import logging

#logging.basicConfig(level=logging.DEBUG)


Int = py_ivar.subtype()


@func((Int), (Int, Int))
def add(x, y):
    return x + y

def genlist():
    nums = [random.randint(0,100) for i in range(1000)]
    #nums = [1,2,4,8]

    return nums, map(Int, nums)


if __name__ == "__main__":
    nums, boundnums = genlist()
    print "sum(nums) = %d" % sum(nums)
    
    print "treereduce(add, bound).get() = %d" % treereduce(add, boundnums).get()
    
    print "dynreduce(add, bound).get() = %d" % dynreduce(add, boundnums).get()
