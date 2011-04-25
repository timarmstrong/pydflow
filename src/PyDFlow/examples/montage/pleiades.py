#!/usr/bin/python

from montage_apps import *
from montage_types import *
import os
import os.path as path
from PyDFlow.base.patterns import resultset

srcdir = os.path.dirname(__file__)

def make_directories(bands):
    try:
        os.mkdir(bands)
    except OSError:
        pass
    try:
        os.mkdir(path.join(bands, "raw"))
    except OSError:
        pass
    try:    
        os.mkdir(path.join(bands, "proj"))
    except OSError:
        pass

def read_img_table(tbl):
    """ Returns a list of (url, image name) pairs from table """
    lines = open(tbl).readlines()[3:] # ignore 3 line header
    for line in lines:
        toks = line.split()
        yield (toks[-2], toks[-1])
            
def remove_ext(path, ext):
    """ Strip given file extension if present """
    n = len(ext)
    if len(path) >= n:
        if path[-1*n:] == ext:
            return path[:-1*n]
        else:
            return path
    
header = MosaicData(path.join(srcdir, "pleiades.hdr"))

# size of montage in degrees
dims = (3,3)

refetch = False

def archive_fetch(bands):
    """
    Ask NASA server which image files we'll need for this patch of
    the sky and these frequency bands
    """
    make_directories(bands)
    img_table = Table(path.join(bands, 'raw', 'remote.tbl'))
    img_table << mArchiveList("dss", bands,"56.5 23.75", dims[0], dims[1])
    img_tables.append(img_table)


def process_one_band(bands, tbl):
    """
    Take the band name and the table of images and
    generate one image file with all the images stitched
    together
    """
    # Download images from server separately
    raw_images = []
    for url, fname in read_img_table(tbl.get()):
        # raw images go in the raw subdirectory
        raw_path = path.join(bands, 'raw', fname)
        raw_image = Image(raw_path)
        if refetch or not path.exists(raw_path): 
            raw_image << mArchiveGet(url)
        raw_images.append(raw_image)
    
    # projected images go in the proj subdirectory
    projected = [Image(path.join(bands, 'proj', 
                    remove_ext(path.basename(r.path()), ".gz")))
                for r in raw_images]
    # Now reproject the images 
    for proj, raw_img in zip(projected, raw_images):
        proj << mProjectPP(raw_img, header)

    # Generate a temporary table with info about images
    proj_table = mImgtbl(*projected)

    # Now combine the projected images into a montage
    band_img = Image(path.join(bands, bands+".fits"))
    band_img << mAdd(proj_table, header, *projected)

# Get info for the three bands we are interested in
allbands = ['DSS2B', 'DSS2R', 'DSS2IR']
img_tables = [archive_fetch(bands)
                for bands in allbands]
    
# For each of the three bands, stitch together the different
# images into a grayscale image.
# Use of resultset forces all futures in img_tables
# , and allows to process the results out of order as
# soon as data is ready.
band_imgs = [process_one_band(bands, tbl)
             for bands, tbl 
             in resultset(img_tables, allbands)]
    
# Make a false-color JPEG image out of the three bands 
res = JPEG("DSS2_BRIR.jpeg") << mJPEGrgb(band_imgs[2], 
                        band_imgs[1], band_imgs[0])
res.get() # Calling get triggers execution
