from PyDFlow.app import *

Header = localfile.subtype()
Image = localfile.subtype()
MosaicData = localfile.subtype()
Table = localfile.subtype()
JPEG = localfile.subtype()
Status = localfile.subtype()



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
