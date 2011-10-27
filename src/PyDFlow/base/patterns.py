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

from __future__ import with_statement
'''
@author: Tim Armstrong
'''
import Queue
from itertools import islice, imap, izip
import logging
import heapq
from PyDFlow.base.mutex import graph_mutex

def resultlist(ivars, max_ready=None):
    """
    Take a bunch of ivars, start them running and return results in 
    in the order provided
    """
    # Use the resultset as the execution mechanism and reassemble into
    # correct order here
    next_id = 0
    
    # Heap of results to be returns
    done = []
    
    class HeapObj:
        def __init__(self, ix, item):
            self.ix = ix
            self.item = item
            
        def __cmp__(self, oth):
            return cmp(self.ix, oth.ix)
    
    for i, res in resultset(ivars, max_ready=max_ready):
        if i == next_id:
            yield res
            next_id += 1
            while len(done) > 0 and done[0].ix == next_id:
                ho = heapq.heappop(done)
                yield ho.item
                next_id += 1
        else:
            heapq.heappush(done, HeapObj(i, res))
    while len(done) > 0:
        yield heapq.heappop(done).item
        
def resultset(ivars, ivar_ids=None, max_ready=None):
    """
    Take a bunch of ivars, start them running and iterate over
    the results in the order they finish.

    max_ready limits the number of tasks that will be launched at
    one time
    """
    finishedq = Queue.Queue()
    def callback(ivar):
        finishedq.put(ivar)

    """
    Start the ivars running and iterate over the results 
    of the ivar in the order in which they finish.
    """
    if ivar_ids is None:
        iter = enumerate(ivars)
    else:
        iter = izip(ivar_ids, ivars)
    
    outstanding = {} 
    noutstanding = 0

    for id, ivar in iter:
        # Bound the number of outstanding requests
        while max_ready and noutstanding >= max_ready:
            # wait for something to finish before running
            finished = finishedq.get()
            fin_id = outstanding.pop(finished)
            noutstanding -= 1
            yield fin_id, finished
        # track the id (assume ivars are uniquely hashable)
        #   which is true if hash not overloaded
        
        # spark and register callback first to avoid race condition
        ivar.spark(done_callback=callback)
        outstanding[ivar] = id
        noutstanding += 1
        # Yield all of the finished items before launching more
        while (not finishedq.empty()):
            finished = finishedq.get()
            fin_id = outstanding.pop(finished)
            noutstanding -= 1
            yield fin_id, finished
    # finished launching all
    while noutstanding > 0: # check not empty
        finished = finishedq.get()
        fin_id = outstanding.pop(finished)
        noutstanding -= 1
        yield fin_id, finished

def treereduce(reducefun, args):
    """
    Associative reduce:
    Reduces the input in pairs in a pattern that forms a tree.
    args can be any finite iterable object
    """
    done = False
    # coerce to a list or tuple: something we can use len with
    if not isinstance(args, list) and not isinstance(args, tuple):
        remaining = list(args)
    else:
        remaining = args

    if len(remaining) == 0:
        raise ValueError("Zero length iterable provided to treereduce")

    while len(remaining) > 1:
        # Build a list of half the size
        end = len(remaining) - (len(remaining) % 2)
        logging.debug("treereduce: %d remaining" % len(remaining))
        remaining2 = map(reducefun, 
                    islice(remaining, 0, end, 2), 
                    islice(remaining, 1, end, 2))

        # Handle extra args
        if len(remaining) % 2 == 1:
            remaining2.append(remaining[-1])
        remaining = remaining2
    return remaining[0]

def dynreduce(reducefun, args):
    """
    reducefun should be a reduce function that takes two arguments.
    It should be commutative and associative.
    args should be an iterable of ivars
    This reducer performs reductions as data becomes available.
    """
    finishedq = Queue.Queue()
    def callback(ivar):
        finishedq.put(ivar)

    nleft = 0
    # Start everything running
    for arg in args:
        arg.spark(done_callback=callback)
        nleft += 1

    # first argument to a reduce call: None if one not present yet
    first = None
    while nleft > 1:
        curr = finishedq.get()
        if first is None:
            first = curr
        else:
            reduced = reducefun(first, curr)
            reduced.spark(done_callback=callback)
            # one less item to reduce 
            nleft -= 1
            first = None
            
    # Last task will be the final product
    return finishedq.get()


def waitall(*args):
    """
    args can be ivars or iterable containers of ivars
    """
    items = []
    next_to_run = 0
    for arg in args:
        try:
            # See if this is iterable
            items.extend(arg)
        except TypeError:
            items.append(arg)
        
        # Force just added items
        # Avoid repeatedly acquiring mutex
        with graph_mutex:
            for i in xrange(next_to_run, len(items)):
                items[i]._spark()
        # keep track of which we have sparked
        next_to_run = len(items)
            
    # Wait for all to finish
    for i in items:
        i.get()

def foldl(fn, init, lst):
    for accum in scanl(fn, init, lst):
        pass
    return accum

def scanl(fn, init, ls):
    # TODO: check that fn has signature (b, a) => b
    # then we can disable type checking for run
    accum = init
    yield accum
    for a in ls:
        accum = fn(accum, a)
        yield accum
