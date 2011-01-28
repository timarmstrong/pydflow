#!/usr/bin/python
from PyDFlow.app import *
import datetime
import sys
import logging 
import PyDFlow.app.paths as app_paths
#logging.basicConfig(level=logging.DEBUG)
app_paths.add_path("./PyDFlow/examples/mergesort/")

intfile = localfile.subtype()
sorted_intfile = localfile.subtype()

app_count = 0

@app((sorted_intfile), (intfile))
def sort(file):
    return "sort -n @file -o @output_0"

@app((sorted_intfile), (sorted_intfile, sorted_intfile))
def merge(f1, f2):
    return "merge @f1 @f2 @output_0"



def merge_sort(unsorted):
    global app_count
    print "sorting %d unsorted files" % (len(unsorted))
    # Sort all the individual files
    sorted = map(sort, unsorted)
    app_count += len(sorted)
    # Merge them all hierarchically
    while len(sorted) > 1:
        odds = sorted[::2]
        evens = sorted[1::2]
        app_count += len(evens)
        next = ([merge(a, b) 
            for a, b 
            in zip(odds, evens)])

        # Check if there was an unpaired one
        if len(sorted) % 2 == 1:
            sorted = next + sorted[-1:]
        else:
            sorted = next
    return sorted[0]

def main():
    start_t = datetime.datetime.now()
    unsorted_files = [intfile.bind(f) for f in sys.argv[1:]]
    output = sorted_intfile.bind("mergesorted.txt")
    output <<= merge_sort(unsorted_files)

    # Need to call get() to initiate the sorting
    graph_built_t = datetime.datetime.now()

    print "%d apps built into graph in %s" % (app_count, graph_built_t - start_t)
    
    output.force()
    fill_started_t = datetime.datetime.now()
    print "Sorted file is being written to at %s" % output.get()
    output.get()
    time_done_t = datetime.datetime.now()
    print "%d apps finished at %s" % (app_count, time_done_t - graph_built_t)


if __name__ == "__main__":
    main()
