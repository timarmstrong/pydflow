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
