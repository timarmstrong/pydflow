import Queue

def resultbag(channels, channel_ids=None, max_running=None):
    """
    Take a bunch of channels, start them running and iterate over
    the results in the order they finish.

    max_running limits the number of tasks that will be launched at
    one time
    """
    finishedq = Queue.Queue()
    def callback(chan):
        finishedq.put(chan)

    """
    Start the channels running and iterate over the results 
    of the channel in the order in which they finish.
    """
    if channel_ids is None:
        iter = enumerate(channels)
    else:
        iter = izip(channel_ids, channels)
    
    outstanding = {} 
    noutstanding = 0
    max_running = max_running
    for id, chan in iter:
        # Bound the number of outstanding requests
        while max_running and noutstanding >= max_running:
            # wait for something to finish before running
            finished = finishedq.get()
            fin_id = outstanding.pop(finished)
            noutstanding -= 1
            yield fin_id, finished
        # track the id (assume channels are uniquely hashable)
        #   which is true if hash not overloaded
        
        # force and register callback first to avoid race condition
        chan.force(done_callback=callback)
        outstanding[chan] = id
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

