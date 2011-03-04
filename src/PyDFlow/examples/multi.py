'''
@author: Tim Armstrong
'''
from PyDFlow.app import *
from PyDFlow.types import *

@app((localfile), (Multiple(localfile)))
def sort(*infiles):
    return "sort -o @output_0 @infiles"

import sys

x = localfile("this.txt")
x <<= sort(*[localfile(arg) for arg in sys.argv[1:]])

x.get()

