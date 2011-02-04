#!/usr/bin/python
import random
import time
from PyDFlow import *
from PyDFlow.PyFun import *

Int = future.subtype()


@func((Int), (Int))
def rand_dur(x):
    """
    Sleep for a random duration and then return the argument.
    """
    dur = random.random() * 1
    time.sleep(dur)
    return x

inp = [Int.bind(i) for i in range(20)]

out = map(rand_dur, inp)

b = ResultBag(out, max_running=2)

def use_rbag(b):    
    res = []
    for i, c in b():
        print i, c.get()
        res.append((i, c.get()))

    print len(res), "results"

print "Testing result collection with fresh channels"
use_rbag(b)
print "Testing result collection with ready channels"
use_rbag(b) # check that it works ok with already finished tasks

inp2 = [Int.bind(i) for i in range(20)]

out2 = map(rand_dur, inp)

b = ResultBag(out2 + out )
print "Testing result collection with mix of channels"
use_rbag(b) # check that it works ok with already finished tasks
