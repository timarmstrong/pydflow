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

from PyDFlow.app import *

MosaicData = localfile.subtype() # Text file with mosaic metadata
MTable = localfile.subtype()
MImage = localfile.subtype() # Image tile in FITS format with image metadata
MStatus = localfile.subtype() # mFitplane status file
JPEG = localfile.subtype() # Image in JPEG format

class RemoteMTable(MTable):
    """
    Text table describing set of images that can be downloaded
    """
    def read_urls(self):
        """ Returns a list of (url, image name) pairs from table """
        lines = self.open().readlines()[3:] # ignore 3 line header
        for line in lines:
            toks = line.split()
            yield (toks[-2], toks[-1])

class BackgroundStruct(object):
    def __init__(self, fname, a, b, c):
#    string fname;
#    float a;
#    float b;
#    float c;
        self.fname = fname
        self.a = a
        self.b = b
        self.c = c


class DiffStruct(object):
    def __init__(self, cntr1, cntr2, plus, minus, diff):
#	int cntr1;
#	int cntr2;
#	Image plus;
#	Image minus;
#	Image diff;
        self.cntr1 = cntr1
        self.cntr2 = cntr2
        self.plus = plus
        self.minus = minus
        self.diff = diff
