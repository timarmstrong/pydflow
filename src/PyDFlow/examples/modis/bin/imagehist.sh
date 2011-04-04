#!/bin/sh
convert $1  -format %c histogram:info:- 2>/dev/null | grep : | sort -nr
