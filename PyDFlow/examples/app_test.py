from PyDFlow.app import *
import logging
import os.path
import sys

logging.basicConfig(level=logging.DEBUG)

image = localfile.subtype()

photo = image.bind(os.path.join(os.path.dirname(sys.argv[0]),
                    "shane.jpeg"))
rotated = image.bind("rotated.jpeg")

@app((image), (image, int))
def rotate(input, angle):
    return "convert -rotate %d @input @output_0" % angle


rotated <<= rotate(photo, 180)
print "File at", rotated.get()

#result = rotate(photo, 180)
#path = result.get()
#print "result in %s" % path

#assign(rotated, rotate, photo, 180)
#rotated.get()

#assign(rotated) << (rotate, (photo, 180))
#assign(rotated) << rotate(photo, 180)
