from montage_types import *
from PyDFlow.app import *

#@app( Image #corr_imgs[],
#    (Table, Table, Multiple(Image))) 
def mBgBatch(img_tbl, corr_tbl, *bg_imgs):
    tmp = Table("back_tmp.tbl")
    tmp << Background_list(img_tbl, corr_tbl);

# GOT UP TO HERE
    BackgroundStruct back_struct[] = readData2(tmp);
    foreach background_entry, i in back_struct
    {
        Image proj_img <single_file_mapper; file = @strcat( @dirname( bg_imgs[i] )+"/", @background_entry.fname )>;
        Image corr_img <regexp_mapper; source = @proj_img, match = ".*\\/(.*)", transform = "corr_dir/\\1">;

        float a = background_entry.a;
        float b = background_entry.b;
        float c = background_entry.c;
        corr_img = mBackground( proj_img, a, b, c );

        corr_imgs[ i ] = corr_img;
    }
}


( Image diff_imgs[] ) mDiffBatch( string src_dir, string dest_dir, Table diff_tbl, MosaicData hdr )
{
    DiffStruct diffs[] <csv_mapper; file = diff_tbl, skip = 1, hdelim="| ">;

    foreach d_entry, i in diffs
    {
        Image img_1 <single_file_mapper; file = @strcat( src_dir+"/", @d_entry.plus )>;
        Image img_2 <single_file_mapper; file = @strcat( src_dir+"/", @d_entry.minus )>;

        Image diff_img <single_file_mapper; file = @strcat( dest_dir+"/", @d_entry.diff )>;
        diff_img = mDiff( img_1, img_2, hdr );

        diff_imgs[ i ] = diff_img;
    }
}


( Table fits_tbl ) mFitBatch( Image diff_imgs[], Table diff_tbl )
{
    Status stats[] <structured_regexp_mapper; source = diff_imgs, match = ".*\\/(.*)", transform = "stat_dir/stat.\\1">;

    Table status_tbl <"stats.tbl">;
    status_tbl = create_status_table( diff_tbl );

    foreach img, i in stats
    {
        stats[ i ] = mFitplane ( diff_imgs[i] );
    }

    fits_tbl = mConcatFit( status_tbl, stats );
}

( Image proj_imgs[] ) mProjectPPBatch( Image raw_imgs[], MosaicData hdr )
{
    foreach img, i in raw_imgs
    {
        Image proj_img <regexp_mapper; source = @img, match = ".*\\/(.*)", transform = "proj_dir/proj_\\1">;
        proj_img = mProjectPP( img, hdr );

        proj_imgs[ i ] = proj_img;
    }
}

( Image proj_imgs[] ) mProjectBatch( Image raw_imgs[], MosaicData hdr )
{
    foreach img, i in raw_imgs
    {
        Image proj_img <regexp_mapper; source = @img, match = ".*\\/(.*)", transform = "proj_dir/proj_\\1">;
//        proj_img = mProjectPP( img, hdr );
        proj_img = mProject( img, hdr );

        proj_imgs[ i ] = proj_img;
    }
}
