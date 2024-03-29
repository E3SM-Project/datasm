#!/bin/bash

note='Usage: dsids_to_archive_map.sh  dsid_list_file [ am=PathToArchiveMap ]'

note='For each input dsid, output either:'
note='    dsid:the corresponding Archive_Map line (may be multiple)'
note=' or dsid:NONE'

dsid_list=$1

archman=`$DSM_GETPATH ARCHIVE_MANAGEMENT`
arch_map="$archman/Archive_Map"

if [ $# -eq 2 ]; then
    arg2=$2
    if [ ${arg2:0:3} == "am=" ]; then
        slen=${#arg2}
        arch_map=${arg2:3: $slen}
    fi
fi

# create an extraction request ticket for each dataset_id
# (dsid = Proj.Model.Exp.Res.Realm.Grid.OutType.Freq.Ens)

for dsid in `cat $dsid_list`; do
    foundlines=`cat $arch_map | fgrep ",${dsid},"`
    if [ -z $foundlines ]; then
        echo $dsid:NONE
        continue
    fi
    for aline in $foundlines; do
        echo "$dsid:$aline"
    done

done

exit 0

