#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Must supply al_list and (optionally, sdep_list)"
    echo ""
    echo "Running archive_path_mapper will destroy and recreate directories:"
    echo "    Holodeck"
    echo "    PathsFound"
    exit 1
fi

al_spec=$1

if [ $# -gt 1 ]; then
    sdep=$2
fi

mv PathsFound/ALE* Prev_PathsFound

tools=`$DSM_GETPATH STAGING_TOOLS`
apm=$tools/archive_path_mapper.py

if [ $# -eq 1 ]; then
    echo "python $apm -a $al_spec"
    python $apm -a $al_spec
else
    echo "python $apm -a $al_spec -s $sdep"
    python $apm -a $al_spec -s $sdep
fi



