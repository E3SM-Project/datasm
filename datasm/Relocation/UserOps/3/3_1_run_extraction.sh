#!/bin/bash

staging=`$DSM_GETPATH DSM_STAGING`
tools=`$DSM_GETPATH STAGING_TOOLS`
archmap=`$DSM_GETPATH ARCHIVE_MANAGEMENT`/Archive_Map

if [ $# -eq 0 ]; then
    echo "Usage: $0 <dsidlist> [prestage] [archmap=<alt_archive_map>]"
    echo "    The default archmap is $archmap"
    echo "    If the order of extractions is important (some large ones take many hours),"
    echo "    the \"prestage\" option will place the tickets into the prestage directory"
    echo "        $staging/prestage"
    echo "    allowing the user to COPY the tickets into the \"pending\" directory manually."
    echo "    (COPY insure the tickets receive fresh creation-times for desired sorting)"
    echo "    Tickets are processed by their creation-time values in the pending directory."
    exit 0
fi

dsidlist=$1
prestage=""

while [ $# -gt 0 ]; do
    # echo "arg1 = $1"
    if [ $1 == "prestage" ]; then
        prestage="prestage"
    fi
    if [ ${1:0:8} == "archmap=" ]; then
        archmap=${1:8}
    fi
    
    shift
done

# echo "$tools/datasm_extract_from_archive.sh $dsidlist $prestage archmap=$archmap"

$tools/datasm_extract_from_archive.sh $dsidlist $prestage archmap=$archmap

exit 0

