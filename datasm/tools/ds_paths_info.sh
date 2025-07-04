#!/bin/bash

dsid=$1

pb_root=`$DSM_GETPATH PUBLICATION_DATA`
wh_root1=`$DSM_GETPATH STAGING_DATA`
sf_root1=`$DSM_GETPATH STAGING_STATUS`

staging=`$DSM_GETPATH DSM_STAGING`
sf_root2=$staging/status_ext

sf_root=""
wh_root=""

project=`echo $dsid | cut -f1 -d.`
if [ $project == "E3SM" ]; then
    sf_root=$sf_root1
    wh_root=$wh_root1
fi
if [ ${project:0:5} == "CMIP6" ]; then
    instid=`echo $dsid | cut -f3 -d.`
    if [[ $instid == "E3SM-Project" || $instid == "UCSB" ]]; then
        sf_root=$sf_root1
        wh_root=$wh_root1
    else
        sf_root=$sf_root2
        wh_root=$wh_root1
    fi
fi

dpth=`echo $dsid | tr . /`
sf_path=$sf_root/${dsid}.status
wh_path=$wh_root/$dpth
pb_path=$pb_root/$dpth

echo "SF_PATH: $sf_path"
echo -n " STATUS: DSID=$dsid: "

if [ -f $sf_path ]; then
    tail -1 $sf_path
else
    echo "NONE"
fi

echo "WH_PATH: $wh_path"
    if [ -d $wh_path ]; then
        vdirs=`ls $wh_path`
        for vdir in $vdirs; do
            if [ ${vdir:0:1} != "v" ]; then
                continue
            fi
            vnum=`ls $wh_path/$vdir | wc -l`
            echo "      $vdir:  $vnum files"
        done
    else
        echo "      (empty)"
    fi

echo "PB_PATH: $pb_path"
    if [ -d $pb_path ]; then
        vdirs=`ls $pb_path`
        for vdir in $vdirs; do
            if [ ${vdir:0:1} != "v" ]; then
                continue
            fi
            vnum=`ls $pb_path/$vdir | wc -l`
            echo "      $vdir:  $vnum files"
        done
    else
        echo "      (empty)"
    fi
