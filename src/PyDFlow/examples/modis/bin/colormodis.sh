#!/bin/sh
# Return a new modis files with the 0-16 pixel values changed to
# colors that reflect the land use of that region. (See legend)
#
# usage: colormodis.sh modis.tif recolored.tif
#

infile=$1
outfile=$2
tmp=`mktemp /tmp/modis.$USER.XXXXXX`
mv $tmp $tmp.tif
tmp=$tmp.tif
map=`mktemp /tmp/colormap.$USER.XXXXXX`

# Create color set

(
  cat |
  sed -e 's/ c.=/ /g' -e 's/[^0-9 ]//g' |
  awk '{printf("#%02x%02x%02x #%02x%02x%02x\n",NR-1,NR-1,NR-1,$1,$2,$3)}') >$map <<EOF
       <Entry c1="32" c2="65" c3="179" c4="255"/>
       <Entry c1="0" c2="106" c3="15" c4="255"/>
       <Entry c1="0" c2="124" c3="37" c4="255"/>
       <Entry c1="0" c2="162" c3="91" c4="255"/>
       <Entry c1="0" c2="161" c3="37" c4="255"/>
       <Entry c1="6" c2="146" c3="40" c4="255"/>
       <Entry c1="158" c2="150" c3="104" c4="255"/>
       <Entry c1="193" c2="196" c3="143" c4="255"/>
       <Entry c1="133" c2="170" c3="91" c4="255"/>
       <Entry c1="177" c2="183" c3="65" c4="255"/>
       <Entry c1="164" c2="208" c3="126" c4="255"/>
       <Entry c1="115" c2="171" c3="174" c4="255"/>
       <Entry c1="204" c2="210" c3="83" c4="255"/>
       <Entry c1="217" c2="0" c3="0" c4="255"/>
       <Entry c1="157" c2="227" c3="110" c4="255"/>
       <Entry c1="182" c2="181" c3="194" c4="255"/>
       <Entry c1="148" c2="148" c3="148" c4="255"/>
EOF

cp $infile $tmp

# output logged to stdout/error is ignored by swift for this app()

while read mval color ; do
  #echo color $mval is $color
  #echo convert $tmp "-fill" "$color" "-opaque" "$mval" $tmp
  convert $tmp "-fill" "$color" "-opaque" "$mval" $tmp
done <$map

#cp $tmp $outfile
convert -thumbnail 300x300 $tmp $outfile

# rm $tmp $map # Keep these for debugging, for now.
