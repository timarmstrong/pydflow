output=$1
shift
inputs=$(ls -1 $*)
montage -label '%f' -font Courier-Regular $inputs $output