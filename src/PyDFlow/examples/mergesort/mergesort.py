#!/usr/bin/python
from PyDFlow.app import *
from PyDFlow.base.patterns import treereduce
import datetime
import sys
import logging 
import PyDFlow.app.paths as app_paths
import os.path


#logging.basicConfig(level=logging.DEBUG)

srcdir = os.path.dirname(__file__)
app_paths.add_path(srcdir)

intfile = localfile.subtype()
sorted_intfile = intfile.subtype()

app_count = 0

@app((sorted_intfile), (intfile))
def sort(file):
    return "sort -n @file -o @output_0"

@app((sorted_intfile), (sorted_intfile, sorted_intfile))
def merge(f1, f2):
    return "merge @f1 @f2 @output_0"

@app((localfile), (localfile))
def compile(src):
    return "gcc -o @output_0 @src"

def do_compile():
    bin = localfile.bind(os.path.join(srcdir, "merge")) <<  \
        compile(localfile.bind(os.path.join(srcdir, "merge.c")))
    bin.get()


def merge_sort(unsorted):
    global app_count
    print "sorting %d unsorted files" % (len(unsorted))
    # Sort all the individual files
    sorted = [sort(f) for f in unsorted]
    app_count += len(sorted)
    # NOTE: could replace below with:
    #return treereduce(merge, sorted)

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
    do_compile()

    filenames = sys.argv[1:]
    if len(filenames) == 0:
        print "USAGE: mergesort.py <files of integers>"
        return
    start_t = datetime.datetime.now()
    output = sorted_intfile.bind("mergesorted.txt")
    output <<= merge_sort([intfile.bind(f) for f in filenames])

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
