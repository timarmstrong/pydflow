#!/usr/bin/python
'''
@author: Tim Armstrong
'''
from PyDFlow.app import app, App, localfile, outfiles
import logging
import os.path

srcdir = os.path.dirname(__file__)
#logging.basicConfig(level=logging.DEBUG)

image = localfile.subtype()

photo = image(os.path.join(srcdir,
                    "shane.jpeg"))

@app((image), (image, int))
def rotate(input, angle):
    return App("convert", "-rotate", angle, input, outfiles[0]) 


rotated = image("rotated.jpeg")
rotated <<= rotate(photo, 180)
print "Wrote rotated version of %s to %s" % (photo.get(), rotated.get())
