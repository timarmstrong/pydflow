from PyDFlow.app import *
from montage_types import *
import os.path as path
from PyDFlow import Multiple

@app((Image) ,(Table, MosaicData, Multiple(Image)))
def mAdd(img_tbl, hdr, *imgs):
    # returns mosaic from adding together images.
    #note: assumes that images are only .fit file in their directory
    mos = outfiles[0]
    return App("mAdd", "-p", path.dirname(imgs[0]), "-n", img_tbl, hdr, mos)


@app((Image) ,(Image, float, float, float))
def mBackground(img, a, b, c):
    bg_img = outfiles[0]
    return App("mBackground", "-n", img, bg_img, a, b, c)


@app((Table), (Table, Table))
def mBgModel(img_tbl, fits_tbl):
    corr_tbl = outfiles[0]
    return App("mBgModel", img_tbl, fits_tbl, corr_tbl)


@app((Table), (Table, Multiple(Status)))
def mConcatFit(status_tbl, *stats):
    fits_tbl = outfiles[0]
    return App("mConcatFit", status_tbl, fits_tbl, path.dirname(stats[0]))


@app((Image), (Image, Image, MosaicData))
def mDiff(proj_img_1, proj_img_2, hdr):
    diff_img = outfiles[0]
    return App("mDiff_wrap", "-n", proj_img_1, proj_img_2, diff_img, hdr)
#    mDiff "-n" @proj_img_1 @proj_img_2 @diff_img @hdr;


@app((Table), (Multiple(Image)))
def mImgtbl(*imgs):
    img_tbl = outfiles[0]
    return App("mImgtbl", path.dirname(imgs[0]), img_tbl)


@app((MosaicData), (Table))
def mMakeHdr(img_tbl):
    hdr = outfiles[0]
    return App("mMakeHdr", img_tbl, hdr)


@app((JPEG), (Image))
def mJPEG(mos_img):
    """
    Convert fit to grayscale
    """
    mos_img_jpg = outfiles[0]
    return App("mJPEG", "-gray", mos_img, "20%", "99.98%", "loglog", "-out",
        mos_img_jpg)

@app((JPEG), (Image, Image, Image))
def mJPEGrgb(rimg, gimg, bimg):
    """
    Convert fit to rgb
    """
    return App("mJPEG", 
        "-red", rimg, "-1s", "99.999%", "gaussian-log",
        "-green", gimg, "-1s", "99.999%", "gaussian-log",
        "-blue", bimg, "-1s", "99.999%", "gaussian-log",
        "-out", outfiles[0])




@app((Image), (Image, MosaicData))
def mProjectPP(raw_img, hdr):
    proj_img = outfiles[0]
    return App("mProjectPP", "-X", raw_img, proj_img, hdr)

@app((Image), (Image, MosaicData))
def mProject(raw_img, hdr):
    proj_img = outfiles[0]
    return App("mProject", "-X", raw_img, proj_img, hdr)


def mArchiveList():
    pass

def mArchiveExec():
    pass

# Not yet converted:

#app ( Status stat ) mFitplane( Image diff_img )
#{
#//    mFitplane "-s" @stat @diff_img;
#    mFitplane_wrap "-s" @stat @diff_img;
#}

#app ( Table diff_tbl ) mOverlaps( Table img_tbl )
#{
#    mOverlaps @img_tbl @diff_tbl;
#}


# Util scripts
#app ( Table back_tbl ) Background_list( Table imgs_tbl, Table corrs_tbl )
#{
#    Background_list @imgs_tbl @corrs_tbl @back_tbl;
#}


#app ( Table stat_tbl ) create_status_table( Table diff_tbl )
#{
#    create_status_table @diff_tbl @stat_tbl;
#}
