#
# Return (simple) stats and filenames for the tiles with the $num highest values of $field
#

stats=$1
tilelist=$2
field=$3
num=$4
shift 4

grep " $field"  $* |
sed -e 's/:/ /' |
sort -n -k +2 |
tail -${num} >$stats

( awk '{print $1}' |
  sed -e 's/landuse.//' -e 's/\..*/.tif/' -e 's,^,/home/wilde/bigdata/data/modis/2002/,' \
) <$stats >$tilelist

exit

login1$ cat topurban.txt 
landuse/h20v04.landuse.byfreq 67312 13 0d
landuse/h28v06.landuse.byfreq 82267 13 0d
landuse/h08v05.landuse.byfreq 92674 13 0d
landuse/h11v04.landuse.byfreq 93702 13 0d
landuse/h13v11.landuse.byfreq 104302 13 0d
landuse/h12v04.landuse.byfreq 110772 13 0d
landuse/h19v04.landuse.byfreq 120908 13 0d
landuse/h27v05.landuse.byfreq 128794 13 0d
landuse/h18v03.landuse.byfreq 142756 13 0d
landuse/h18v04.landuse.byfreq 146486 13 0d

login1$ cat urbantiles.txt 
/home/wilde/bigdata/data/modis/2002landuse/h20v04.tif
/home/wilde/bigdata/data/modis/2002landuse/h28v06.tif
/home/wilde/bigdata/data/modis/2002landuse/h08v05.tif
/home/wilde/bigdata/data/modis/2002landuse/h11v04.tif
/home/wilde/bigdata/data/modis/2002landuse/h13v11.tif
/home/wilde/bigdata/data/modis/2002landuse/h12v04.tif
/home/wilde/bigdata/data/modis/2002landuse/h19v04.tif
/home/wilde/bigdata/data/modis/2002landuse/h27v05.tif
/home/wilde/bigdata/data/modis/2002landuse/h18v03.tif
/home/wilde/bigdata/data/modis/2002landuse/h18v04.tif
login1$ fg
