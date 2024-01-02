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


    if [ $section != "COMMON" ]; then
        echo "Skipping section $section"
        continue
    fi

    reloc="$relocdir/$roottag"
    mkdir -p $reloc

    if [ $tailtyp == "FILE" ]; then
        cp $srcpath/$content $reloc
    elif [ $tailtyp == "DIRNAME" ]; then
        mkdir -p $reloc/$content
    elif [ $tailtyp == "PATHTO_FILE" ]; then
        extpath=`dirname $content`
        fullpath=$srcpath/$extpath
        content=`basename $content`
        mkdir -p $reloc/$extpath
        cp $fullpath/$content $reloc/$extpath
    elif [ $tailtyp == "PATHTO_DIRNAME" ]; then
        mkdir -p $reloc/$content
    fi

done
