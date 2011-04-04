#!/bin/sh
# Return (simple) stats and filenames for the tiles with the $num highest values of $field
#

stats=$1
tilelist=$2
field=$3
num=$4
modisdir=$5
shift 5

grep " $field"  $* |
sed -e 's/:/ /' |
sort -n -k +2 |
tail -${num} >$stats

( awk '{system("basename " $1)}' |
  sed -e 's/\.landuse\.byfreq/.tif/' -e "s,^,$modisdir/,"
) <$stats >$tilelist

