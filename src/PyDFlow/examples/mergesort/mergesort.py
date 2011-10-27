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

app_count = 0

intfile = localfile.subtype()
sorted_intfile = intfile.subtype()


@app((sorted_intfile), (intfile))
def sort(file):
    return App("sort", "-n", file, "-o", outfiles[0])

@app((sorted_intfile), (sorted_intfile, sorted_intfile))
def merge(f1, f2):
    return App("merge", f1, f2, outfiles[0])

@app((localfile), (localfile))
def compile(src):
    return App("gcc", "-o", outfiles[0], src)

def do_compile():
    bin = localfile(os.path.join(srcdir, "merge")) <<  \
        compile(localfile(os.path.join(srcdir, "merge.c")))
    bin.get()


def merge_sort(unsorted):
    global app_count
    print "sorting %d unsorted files" % (len(unsorted))
    # Sort all the individual files
    sorted = [sort(f) for f in unsorted]
    app_count += len(sorted)
    return treereduce(merge, sorted)

    # Note: could also code as below:.
    # Merge them all hierarchically
    """
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
    """

def main():
    do_compile()

    filenames = sys.argv[1:]
    if len(filenames) == 0:
        print "USAGE: mergesort.py <files of integers>"
        return
    start_t = datetime.datetime.now()
    output = sorted_intfile("mergesorted.txt")
    output <<= merge_sort([intfile(f) for f in filenames])

    # Need to call get() to initiate the sorting
    graph_built_t = datetime.datetime.now()

    print "%d apps built into graph in %s" % (app_count, graph_built_t - start_t)
    
    output.spark()
    fill_started_t = datetime.datetime.now()
    print "Sorted file is being written to at %s" % output.get()
    output.get()
    time_done_t = datetime.datetime.now()
    print "%d apps finished at %s" % (app_count, time_done_t - graph_built_t)


if __name__ == "__main__":
    main()
