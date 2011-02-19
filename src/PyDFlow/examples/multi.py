from PyDFlow.app import *
from PyDFlow.types import *

@app((localfile), (Multiple(localfile)))
def sort(*infiles):
    return "sort -o @output_0 @infiles"

import sys

x = localfile.bind("this.txt")
x <<= sort(*[localfile.bind(arg) for arg in sys.argv[1:]])

x.get()

