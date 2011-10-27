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
from PyDFlow import *
from PyDFlow.app import * 
import os.path 
import PyDFlow.app.paths as app_paths
import sys

import logging
logging.basicConfig(level=logging.INFO)

if len(sys.argv) not in (2, 3):
    print "USAGE: modis.py <directory with modis *.tif files> [optional output directory]" 
    exit(1)
modisdir = sys.argv[1]
if len(sys.argv) == 3:
    outputdir = sys.argv[2]
else:
    outputdir = modisdir


# Add the modis shell scripts to PyDFlow search path
srcdir = os.path.dirname(__file__)
app_paths.add_path(os.path.join(srcdir, "bin"))



imagefile = localfile.subtype()

landuse = localfile.subtype()

@app((landuse), (imagefile, int)) 
def getLandUse(input, sortfield):
    return App("getlanduse.sh", input, sortfield, stdout=outfiles[0])

@app((localfile, localfile), (int, int, Multiple(landuse)))
def analyzeLandUse(usetype, maxnum, *inputs):
    return App("analyzelanduse.sh", outfiles[0], outfiles[1], usetype, maxnum, modisdir, *inputs)

@app((imagefile), (imagefile))
def colormodis(input):
    return App("colormodis.sh", input, outfiles[0])

# Mappers return read-only array.  Both also can take keyword arguments
geos = GlobMapper(imagefile, modisdir + "/*.tif")
land = SubMapper(landuse, geos, "(h..v..).*$", "\\1.landuse.byfreq", directory=outputdir)

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
bigurban = localfile(os.path.join(outputdir, "topurban.txt"))
urbantiles = localfile(os.path.join(outputdir, "urbantiles.txt"))

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
    recoloredPath = os.path.join(
                    outputdir, 
                    os.path.basename(uf.path()).replace(".tif", ".recolored.tif"))
    recolored = imagefile(recoloredPath) << colormodis(uf)
    recoloredImages.append(recolored)

# Start everything running and wait until completion
waitall(recoloredImages)
