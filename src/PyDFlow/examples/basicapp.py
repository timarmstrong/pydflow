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
