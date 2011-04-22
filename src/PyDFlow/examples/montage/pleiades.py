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


def process_bands(bands, header, raw_images):
    
    projected = [Image(path.join(bands, 'proj', remove_ext(path.basename(r.path()), ".gz")))
                    for r in raw_images] 
    
    SubMapper(Image, raw_images, "raw", "proj")

    for proj, raw_img in zip(projected, raw_images):
        #TODO: ProjectPP?
        #projected[i] << mProject(header, raw_img)
        proj << mProjectPP(raw_img, header)

    proj_table = Table(path.join(bands, 'proj', "pimages.tbl")) << mImgtbl(*projected)

    band_img = Image(path.join(bands, bands+".fits"))
    band_img << mAdd(proj_table, header, *projected)
    return band_img

    
header = MosaicData(path.join(srcdir, "pleiades.hdr"))
allbands = ['DSS2B', 'DSS2R', 'DSS2IR']
img_tables = []
band_imgs = []

fetch = False
if fetch:
    for bands in allbands:
        make_directories(bands)
        img_table = Table(path.join(bands, 'raw', 'remote.tbl'))
        img_table << mArchiveList("dss", bands,"56.5 23.75", dims[0], dims[1])
        img_tables.append(img_table)
    
    for bands, tbl in resultset(img_tables, allbands):
        # Download images separately by splitting table
        x = list(read_remote_table(tbl.get()))
        raw_images = [Image(path.join(bands, 'raw', fname)) << mArchiveGet(url)
                for url, fname in  x]
        band_imgs.append(process_bands(bands, header, raw_images))
else:
    for bands in allbands:
        raw_images = GlobMapper(Image, path.join(bands, "raw/*.fits.gz"))    
        band_imgs.append(process_bands(bands, header, raw_images))
    
    
res = JPEG("DSS2_BRIR.jpeg") << mJPEGrgb(band_imgs[2], 
                        band_imgs[1], band_imgs[0])
res.get()
