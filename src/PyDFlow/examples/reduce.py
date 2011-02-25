#!/usr/bin/python

from PyDFlow import *
from PyDFlow.PyFun import *
import random
import logging

#logging.basicConfig(level=logging.DEBUG)


Int = future.subtype()


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
