#!/usr/bin/python
from PyDFlow.app import *
import logging
import os.path
import sys

srcdir = os.path.dirname(__file__)
#logging.basicConfig(level=logging.DEBUG)

image = localfile.subtype()

photo = image.bind(os.path.join(srcdir,
                    "shane.jpeg"))

@app((image), (image, int))
def rotate(input, angle):
    return "convert -rotate %d @input @output_0" % angle


rotated = image.bind("rotated.jpeg")
rotated <<= rotate(photo, 180)
print "Wrote rotated version of %s to %s" % (photo.get(), rotated.get())
