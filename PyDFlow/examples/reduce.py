#!/usr/bin/python

from PyDFlow import *
from PyDFlow.PyFun import *
import random
import logging

#logging.basicConfig(level=logging.DEBUG)


Int = future.subtype()

nums = [random.randint(0,100) for i in range(1000)]
#nums = [1,2,4,8]

bound = map(Int.bind, nums)

@func((Int), (Int, Int))
def add(x, y):
    return x + y

print "sum(nums) = %d" % sum(nums)

print "treereduce(add, bound).get() = %d" % treereduce(add, bound).get()

print "dynreduce(add, bound).get() = %d" % dynreduce(add, bound).get()
