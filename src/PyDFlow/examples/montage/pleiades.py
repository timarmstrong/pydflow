from montage_apps import *
from montage_types import *
import os
import os.path as path


dims = (3,3)

def main():
    header = Header("pleaides.hdr")
    allbands = ['DSS2B', 'DSS2R', 'DSS2IR']
    band_imgs = []
    for bands in allbands:
        band_imgs.append(process_bands(bands, header))
    res = JPEG("DSS2_BRIR") << mJPEGrgb(band_imgs[2], 
                            band_imgs[1], band_imgs[0])
    res.get()


def process_bands(bands, header):
    print "Processing", bands
    make_directories(bands)

    img_table = Table('remote.tbl')

    img_table << mArchiveList("dss", bands,"56.5 23.75", dims[0], dims[1]);
    mArchiveExec(img_table)

    raw_images = GlobMapper(Image, path.join(bands, "raw/*.fit"))
    projected = SubMapper(Image, raw_images, "raw", "proj")

    for i, raw_img in enumerate(raw_images):
        #TODO: ProjectPP?
        projected[i] << mProject(header, raw_img)

    proj_table = Table("pimages.tbl") << mImgtbl(*projected)

    band_img = Image(path.join(bands, bands+".fits"))
    band_img << mAdd(proj_table, header, *projected)
    return band_img


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
        os.mkdir(path.join(bands, "projected"))
    except OSError:
        pass
