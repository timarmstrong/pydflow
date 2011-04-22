from montage_apps import *
from montage_types import *
import os
import os.path as path
from PyDFlow.base.patterns import resultset
import logging

dims = (3,3)
srcdir = os.path.dirname(__file__)


logging.basicConfig(level=logging.INFO)




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




def read_remote_table(tbl):
    """
    Returns a list of (url, image name) pairs from table
    """
    i = 0
    for line in open(tbl).readlines():
        # 3 line header
        if i < 3:
            i += 1
        else:
            toks = line.split()
            filename = toks[-1]
            url = toks[-2]
            yield (url, filename)
            
def remove_ext(path, ext):
    n = len(ext)
    if len(path) >= n:
        if path[-1*n:] == ext:
            return path[:-1*n]
        else:
            return path

    
header = MosaicData(path.join(srcdir, "pleiades.hdr"))
allbands = ['DSS2B', 'DSS2R', 'DSS2IR']
img_tables = []
band_imgs = []

refetch = False

# Find out the image files we'll need
for bands in allbands:
    make_directories(bands)
    img_table = Table(path.join(bands, 'raw', 'remote.tbl'))
    img_table << mArchiveList("dss", bands,"56.5 23.75", dims[0], dims[1])
    img_tables.append(img_table)
    
for bands, tbl in resultset(img_tables, allbands):
    # Download images separately
    raw_images = []
    for url, fname in read_remote_table(tbl.get()):
        raw_path = path.join(bands, 'raw', fname)
        raw_image = Image(raw_path)
        if refetch or not path.exists(raw_path): 
            raw_image << mArchiveGet(url)
        raw_images.append(raw_image)
    
        projected = [Image(path.join(bands, 'proj', remove_ext(path.basename(r.path()), ".gz")))
                    for r in raw_images] 
    
    # Now project the images
    SubMapper(Image, raw_images, "raw", "proj")

    for proj, raw_img in zip(projected, raw_images):
        #TODO: ProjectPP?
        #projected[i] << mProject(header, raw_img)
        proj << mProject(raw_img, header)

    proj_table = Table(path.join(bands, 'proj', "pimages.tbl")) 
    proj_table << mImgtbl(*projected)

    # Now add the projected images
    band_img = Image(path.join(bands, bands+".fits"))
    band_img << mAdd(proj_table, header, *projected)

    band_imgs.append(band_img)
    
    
res = JPEG("DSS2_BRIR.jpeg") << mJPEGrgb(band_imgs[2], 
                        band_imgs[1], band_imgs[0])
res.get()
