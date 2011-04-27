from PyDFlow.app import *
from montage_types import *
import os.path as path
from PyDFlow import Multiple


import PyDFlow.app.paths as app_paths

# Add the montage binaries to PyDFlow search path
srcdir = path.dirname(__file__)
app_paths.add_path(path.join('/var/tmp/code/Montage_v3.3', "bin"))
app_paths.add_path('/var/tmp/code/SwiftApps/Montage/scripts')

@app((MImage) ,(MTable, MosaicData, Multiple(MImage)))
def mAdd(img_tbl, hdr, *imgs):
    # returns mosaic from adding together images.
    #note: assumes that images are only .fit file in their directory
    mos = outfiles[0]
    return App("mAdd", "-p", path.dirname(imgs[0]), "-n", img_tbl, hdr, mos)


@app((MImage) ,(MImage, float, float, float))
def mBackground(img, a, b, c):
    bg_img = outfiles[0]
    return App("mBackground", "-n", img, bg_img, a, b, c)


@app((MTable), (MTable, MTable))
def mBgModel(img_tbl, fits_tbl):
    corr_tbl = outfiles[0]
    return App("mBgModel", img_tbl, fits_tbl, corr_tbl)


@app((MTable), (MTable, Multiple(MStatus)))
def mConcatFit(status_tbl, *stats):
    fits_tbl = outfiles[0]
    return App("mConcatFit", status_tbl, fits_tbl, path.dirname(stats[0]))


@app((MImage), (MImage, MImage, MosaicData))
def mDiff(proj_img_1, proj_img_2, hdr):
    diff_img = outfiles[0]
    return App("mDiff_wrap", "-n", proj_img_1, proj_img_2, diff_img, hdr)
#    mDiff "-n" @proj_img_1 @proj_img_2 @diff_img @hdr;


@app((MTable), (Multiple(MImage)))
def mImgtbl(*imgs):
    img_tbl = outfiles[0]
    return App("mImgtbl", path.dirname(imgs[0]), img_tbl)


@app((MosaicData), (MTable))
def mMakeHdr(img_tbl):
    hdr = outfiles[0]
    return App("mMakeHdr", img_tbl, hdr)


@app((JPEG), (MImage))
def mJPEG(mos_img):
    """
    Convert fit to grayscale
    """
    mos_img_jpg = outfiles[0]
    return App("mJPEG", "-gray", mos_img, "20%", "99.98%", "loglog", "-out",
        mos_img_jpg)

@app((JPEG), (MImage, MImage, MImage))
def mJPEGrgb(rimg, gimg, bimg):
    """
    Convert fit to rgb
    """
    return App("mJPEG", 
        "-red", rimg, "-1s", "99.999%", "gaussian-log",
        "-green", gimg, "-1s", "99.999%", "gaussian-log",
        "-blue", bimg, "-1s", "99.999%", "gaussian-log",
        "-out", outfiles[0])




@app((MImage), (MImage, MosaicData))
def mProjectPP(raw_img, hdr):
    proj_img = outfiles[0]
    return App("mProjectPP", "-X", raw_img, proj_img, hdr)

@app((MImage), (MImage, MosaicData))
def mProject(raw_img, hdr):
    proj_img = outfiles[0]
    return App("mProject", "-X", raw_img, proj_img, hdr)

@app((RemoteMTable), (str, str, str, None, None ))
def mArchiveList(survey, band, obj_or_loc, width, height):
    """
    From montage docs:
    survey: e.g.
        * 2MASS
        * DSS
        * SDSS
        * DPOSS
    band:
        Case insensitive - can be one of:
        * (2MASS) j, h, k
        * (SDSS) u, g, r, i, z
        * (DPOSS) f, j, n
        * (DSS) DSS1, DSS1R, DSS1B, DSS2, DSS2B, DSS2R, DSS2IR
    object|location:
        Object name or coordinate string to be resolved by NED
    width:  
        Width of area of interest, in degrees
    height:
        Height of area of interest, in degrees
    """
    return App("mArchiveList", survey, band, obj_or_loc, width, height, outfiles[0])

@app((localfile), (MTable))
def mArchiveExec(imgtbl):
    """
    Return value is directory
    """
    return App("mArchiveExec_wrap", imgtbl, outfiles[0])

@app((MImage), (str))
def mArchiveGet(url):
    """
    Get an image from repo
    """
    return App("mArchiveGet", url, outfiles[0])


@app((MStatus), (MImage))
def mFitplane(diff_img):
    return App("mFitplane", "-s", outfiles[0], diff_img)

@app((MTable), (MTable))
def mOverlaps(img_tbl):
    return App("mOverlaps", img_tbl, outfiles[0])

# Util scripts
#app ( MTable back_tbl ) Background_list( MTable imgs_tbl, MTable corrs_tbl )
#{
#    Background_list @imgs_tbl @corrs_tbl @back_tbl;
#}


#app ( MTable stat_tbl ) create_status_table( MTable diff_tbl )
#{
#    create_status_table @diff_tbl @stat_tbl;
#}
