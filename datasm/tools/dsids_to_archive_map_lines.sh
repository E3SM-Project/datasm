#!/bin/bash

note='Usage: dsids_to_archive_map.sh  dsid_list_file'

note='For each input dsid, output either:'
note='    dsid:the corresponding Archive_Map line (may be multiple)'
note=' or dsid:NONE'

dsid_list=$1

arch_map="/p/user_pub/e3sm/archive/.cfg/Archive_Map"
dsid_am_key=/p/user_pub/e3sm/staging/tools/am_key_from_dsid.sh


# create an extraction request ticket for each dataset_id
# (dsid = Proj.Model.Exp.Res.Realm.Grid.OutType.Freq.Ens)

for dsid in `cat $dsid_list`; do
    key=`$dsid_am_key $dsid`
    foundlines=`cat $arch_map | grep $key`
    if [ -z $foundlines ]; then
        echo $dsid:NONE
        continue
    fi
    for aline in $foundlines; do
        echo "$dsid:$aline"
    done

done

exit 0

