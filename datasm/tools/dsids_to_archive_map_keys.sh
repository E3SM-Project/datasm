#!/bin/bash

note='Usage: dsids_to_archive_map_keys.sh  dsid_list_file'

dsid_list=$1

dsid_am_key=/p/user_pub/e3sm/staging/tools/am_key_from_dsid.sh

# create an extraction request ticket for each dataset_id
# (dsid = Proj.Model.Exp.Res.Realm.Grid.OutType.Freq.Ens)

for dsid in `cat $dsid_list`; do
    $dsid_am_key $dsid

done

exit 0

