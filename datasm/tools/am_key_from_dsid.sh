#!/bin/bash

note='Usage: am_key_from_dsid.sh dataset_id'

dsid=$1

# create an archive map key from a dataset_id
# (dsid = Proj.Model.Exp.Res.Realm.Grid.OutType.Freq.Ens)

pat_1=`echo $dsid | cut -f2-4 -d. | tr . ,`
realm=`echo $dsid | cut -f5 -d.`
grid=`echo $dsid | cut -f6 -d.`
otyp=`echo $dsid | cut -f7 -d.`
freq=`echo $dsid | cut -f8 -d.`
ensm=`echo $dsid | cut -f9 -d.`

if [ $grid == "native" ]; then
    grid="nat"
fi
if [ $realm == "atmos" ]; then
    realm="atm"
elif [ $realm == "land" ]; then
    realm="lnd"
elif [ $realm == "ocean" ]; then
    realm="ocn"
fi

dtype="${realm}_${grid}_${freq}"
key="$pat_1,$ensm,$dtype,$otyp"

echo $key


exit 0

