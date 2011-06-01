#!/usr/bin/python
'''
@author: Tim Armstrong
'''
import datetime
from PyDFlow.PyFun import *
from PyDFlow import *
import logging
import random

#logging.basicConfig(level=logging.DEBUG)

Int = py_ivar.subtype()

@func((Int), ())
def zero():
    return 0

@func((Int), ())
def one():
    return 1

@func((Int),(Int, Int))
def add(a, b):
    return a + b

@func((Int), (Int, None))
def add1(a, b):
    return a + b

@func((Int), (Int, Int))
def nextfib(f1, f2):
    return f1 + f2

@func((Int), (Multiple(Int)))
def maxi(*args):
    return max(args)

def fib(n):
    f1 = Int(0) # fib(0)
    if n == 0:
        return f1
    f2 = Int(1)  # fib(1)
    if n == 1:
        return f2
    for i in range(n - 1):
        f3 = nextfib(f1, f2)
        f1 = f2
        f2 = f3
    return f2

sorted_array = py_ivar.subtype()

@func((sorted_array), (sorted_array, sorted_array))
def merge(l1, l2):
    i = 0
    j = 0
    n1 = len(l1) 
    n2 = len(l2)
    res = []
    while i < n1 and j < n2:
        if l1[i] <= l2[j]:
            res.append(l1[i])
            i += 1
        else:
            res.append(l2[j])
            j += 1
    if i < n1:
        res = res + l1[i:]
    elif j < n2:
        res = res + l2[j:]
    return res

def merge_sort(xs):
    app_count = 0
    start_t = datetime.datetime.now()
    # list of lists - 
    # each of this individual lists is sorted (as it only has one element)
    lol = [sorted_array([x])
                for x in xs]
    while len(lol) > 1:
        odds = lol[::2]
        evens = lol[1::2]
        app_count += len(evens)
        # Check if we can pair all up
        if len(lol) % 2 == 1:
            spare = lol[-1:]
        else:
            spare = []
        lol = ([merge(a, b) 
            for a, b 
            in zip(odds, evens)]
            + spare)
#        print "Down to level of %d numbers" % len(lol)
    graph_built_t = datetime.datetime.now()
#    print "%d apps built into graph in %s" % (app_count, graph_built_t - start_t)
    return lol[0]
        
    
def main():
    #logging.basicConfig(level=logging.DEBUG)
    print "One(): %d" % one().get()
    a = one()
    print "a = 1; a + a = %d" % add(a,a).get()
    print "1 + 1 = %d" % add(one(), one()).get()
    print "1 + (1 + 1) = %d" % add(one(), add(one(), one())).get()
    print "1 + 2 = %d" % add(Int(1), Int(2)).get()
    
    for n in range(10):
        print "Fib(%d) = %d" % (n, fib(n).get())
    
    xs = [random.randint(0, 10000) for i in range(100)]
    print "sorted: %s" % repr(merge_sort(xs).get())

    x = Int()
    x <<= add(a,a)
    print "x = %d" % x.get()


    print "maxi(4,2,6) = %d" % maxi(Int(4), Int(2), Int(6)).get()

    print "Done."

if __name__ == "__main__":
    main()
