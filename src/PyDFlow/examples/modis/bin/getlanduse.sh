#
# Read a MODIS .tif file and return a histogram of pixel values.
# The pixel values signify land use for that region (see legend in notes)
#
# Usage: getlanduse.sh modisFile histogramFile
#

sleep 10
convert  $1 -format %c histogram:info:- 2>/dev/null |
  grep '[0-9]' |
  sort -nr |
  sed -e 's/[^0-9 ]//g' |
  awk '{print $1, $3, sprintf("%02x",$3)}' |
  sort -n -k $2
