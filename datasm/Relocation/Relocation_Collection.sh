#!/bin/bash

manifest=$1
relocdir=$2

mkdir -p $relocdir

for aline in `cat $manifest`; do

    section=`echo $aline | cut -f1 -d,`
    roottag=`echo $aline | cut -f2 -d,`
    tailtyp=`echo $aline | cut -f3 -d,`
    srcpath=`echo $aline | cut -f4 -d,`
    content=`echo $aline | cut -f5 -d,`


    mkdir -p $relocdir/USER_ROOT

    if [ $section == "COMMON" ]; then
        relocdst="$relocdir/$roottag"
        mkdir -p $relocdst
        if [ $tailtyp == "FILE" ]; then
            cp $srcpath/$content $relocdst
        elif [ $tailtyp == "DIRNAME" ]; then
            mkdir -p $relocdst/$content
        elif [ $tailtyp == "PATHTO_FILE" ]; then
            extpath=`dirname $content`
            fullpath=$srcpath/$extpath
            content=`basename $content`
            mkdir -p $relocdst/$extpath
            cp $fullpath/$content $relocdst/$extpath
        elif [ $tailtyp == "PATHTO_DIRNAME" ]; then
            mkdir -p $relocdst/$content
        fi
    elif [ $section == "USEROP" ]; then
        dstp=$relocdir/USER_ROOT/$roottag
        srcp=$srcpath
        mkdir -p $dstp
        if [ $tailtyp == "FILE" ]; then
            cp $srcpath/$content $dstp
        elif [ $tailtyp == "DIRNAME" ]; then
            mkdir -p $dstp/$content
        elif [ $tailtyp == "PATHTO_FILE" ]; then
            extpath=`dirname $content`
            dstp=$relocdir/USER_ROOT/$roottag/$extpath
            mkdir -p $dstp
            content=`basename $content`
            srcp=$srcpath/$extpath
            cp $srcp/$content $dstp
        elif [ $tailtyp == "PATHTO_DIRNAME" ]; then
            dstp=$relocdir/USER_ROOT/$roottag/$content
            mkdir -p $dstp
        fi
        
    fi

done

dsm_stp=`$DSM_GETPATH DSM_STAGING`
sed -i "s%$dsm_stp%RELOC_HOME%" $relocdir/DSM_STAGING/.dsm_get_root_path.sh
sed -i "s%:%:[SAMPLE_PATH]%g" $relocdir/DSM_STAGING/.dsm_root_paths

# cp DSM_Deployment_Instructions $relocdir/1_DSM_Deployment_Instructions
