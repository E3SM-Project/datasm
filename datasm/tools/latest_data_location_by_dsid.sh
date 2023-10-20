#!/bin/bash

comm="If elements of input dsid exist in either warehouse or publication, then"
comm="return the full path to the lastest populated version directory, else NONE"

dsid=$1

wh_root=`$DSM_GETPATH STAGING_DATA`
pb_root=`$DSM_GETPATH PUBLICATION_DATA`

ds_path=`echo $dsid | tr . /`

wh_path="$wh_root/$ds_path"
pb_path="$pb_root/$ds_path"

if [ -d $wh_path ]; then
    if [ -d $pb_path ]; then
        the_path="BOTH"
    else
        the_path=$wh_path
    fi
elif [ -d $pb_path ]; then
    the_path=$pb_path
else
    echo "NONE"
    exit 0
fi
    
if [ $the_path != "BOTH" ]; then
    lastleaf=`ls $the_path | tail -1`
    if [ "X$lastleaf" == "X" ]; then    # no leaf
        echo "NONE"
        exit 0
    fi
    fullpath="$the_path/$lastleaf"
    echo $fullpath
    exit 0
fi

# must choose the better source 

wh_lastleaf=`ls $wh_path | tail -1`
pb_lastleaf=`ls $pb_path | tail -1`

if [ "X$wh_lastleaf" == "X" ]; then
    if [ "X$pb_lastleaf" == "X" ]; then
        echo "NONE"
        exit 0
    fi
    fullpath="$pb_path/$pb_lastleaf"
elif [ "X$pb_lastleaf" == "X" ]; then
    if [ "X$wh_lastleaf" == "X" ]; then
        echo "NONE"
        exit 0
    fi
    fullpath="$wh_path/$wh_lastleaf"
else
    fullpath="BOTH"
fi

if [ $fullpath != "BOTH" ]; then
    echo $fullpath
    exit 0
fi

# choose the latest populated path

full_wh_path="$wh_path/$wh_lastleaf"
full_pb_path="$pb_path/$pb_lastleaf"

wh_count=`ls $full_wh_path | wc -l`
pb_count=`ls $full_pb_path | wc -l`

if [ $wh_count -eq 0 ]; then
    if [ $pb_count -eq 0 ]; then
        echo "NONE"
        exit 0
    fi
    echo $full_pb_path
    exit 0
elif [ $pb_count -eq 0 ]; then
    echo $full_wh_path
    exit 0
fi

# both populated. select latest

if [[ $wh_lastleaf < $pb_lastleaf ]]; then
    fullpath="$pb_path/$pb_lastleaf"
else
    fullpath="$wh_path/$wh_lastleaf"
fi
echo $fullpath

exit 0
