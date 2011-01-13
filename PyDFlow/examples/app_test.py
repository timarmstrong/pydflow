from app import *
import logging

logging.basicConfig(level=logging.DEBUG)

image = swfile.subtype()

photo = image.map("shane.jpeg")
rotated = image.map("rotated.jpeg")

@app((image), (image, None))
def rotate(input, angle):
    return "convert -rotate %d @input @output_0" % angle


rotate(photo, 180, _out=rotated)
rotated.get()

#result = rotate(photo, 180)
#path = result.get()
#print "result in %s" % path

#assign(rotated, rotate, photo, 180)
#rotated.get()

#assign(rotated) << (rotate, (photo, 180))
#assign(rotated) << rotate(photo, 180)
