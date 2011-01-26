from PyDFlow.app import *
import datetime
import sys
import logging 
logging.basicConfig(level=logging.DEBUG)

app_count = 0
start_t = datetime.datetime.now()

intfile = flfile.subtype()
sorted_intfile = flfile.subtype()

unsorted_files = [intfile.bind(f) for f in sys.argv[1:]]
print "sorting %d unsorted files" % (len(unsorted_files))

@app((sorted_intfile), (intfile))
def sort(file):
    return "sort -n @file -o @output_0"

@app((sorted_intfile), (sorted_intfile, sorted_intfile))
def merge(f1, f2):
    #TODO: ugly
    return "./PyDFlow/examples/mergesort/merge @f1 @f2 @output_0"

# Sort all the individual files
sorted = map(sort, unsorted_files)
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

# Need to call get() to initiate the sorting

graph_built_t = datetime.datetime.now()

print "%d apps built into graph in %s" % (app_count, graph_built_t - start_t)

sorted[0].force()
fill_started_t = datetime.datetime.now()
print "Sorted file is being written to at %s" % sorted[0].get()
sorted[0].get()
time_done_t = datetime.datetime.now()
print "%d apps finished at %s" % (app_count, time_done_t - graph_built_t)
