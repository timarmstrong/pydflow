import re #regex module
from PyDFlow import *
from PyDFlow.app import * 


imagefile = localfile.subtype()
landuse = localfile.subtype()

@app((landuse), (imagefile, int)) 
def getLandUse(input, sortfield):
    return App("getlanduse", input, sortfield, stdout=output)

@app((localfile, localfile), (int, int, Multiple(landuse)))
def analyzeLandUse(usetype, maxnum, *inputs):
    return App("analyzelanduse", outfiles[0], outfiles[1], usetype, maxnum, *inputs)

@app((imagefile), (imagefile))
def colormodis(input):
    return App("colormodis", input, outputs[0])

# Both read only arrays
geos = GlobMapper(imagefile, "/home/wilde/bigdata/data/modis*.tif")
land = SubMapper(landuse, geos, "(h..v..)", "\\1.landuse.byfreq")

# Find the land use of each modis tile
# enumerate is a python built-in which generates indices
for i, g in enumerate(geos):
    # << operator assigns to mapped file
    land[i] << getLandUse(g,1);
"""
Alternative 1 - For Loop
land = []
for geo in geos:
    newpath = re.sub("(h..v..)", "\\1.landuse.byfreq", geo.path())
    land.append(landuse(newpath))

Alternative 2 - Array Comprehension
pattern, transform = "(h..v..)", "\\1.landuse.byfreq"
land = [landuse(re.sub(pattern, transform, geo.path()))
        for geo in geos]
"""

# Find the top 10 most urban tiles (by area)
UsageTypeURBAN=13;
bigurban = localfile("topurban.txt")
urbantiles = localfile("urbantiles.txt")

#TODO: << to handle tuples
(bigurban, urbantiles) << analyzeLandUse(UsageTypeURBAN, 10, *land);

# Map the files to an array.  
# NOTE: script blocks here on the open() command until urbantiles available
urbanfilenames = [line.strip() for line in urbantiles.open().readlines()]

# array_mapper is redundant: use built-in map to apply imagefile constructor
# to filenames
urbanfiles = map(imagefile, urbanfilenames) 

# Create a set of recolored images for just the urban tiles

recoloredImages = []
for uf in urbanfiles:
    recoloredPath = uf.path().replace(".tif", ".recolored.tif")
    recoloredImages.append(imagefile(recoloredPath) << colormodis(uf))

# Start everything running and wait until completion
getall(recoloredImages)
