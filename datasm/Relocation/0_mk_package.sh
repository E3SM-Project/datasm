#!/bin/bash

rm -rf RELOC
rm -f *.gz

ts=`date -u +%Y%m%d_%H%M%S_%6N`
manifest=DataSM_System_Local_Manifest-$ts
pkg_log=Package_Log-$ts

thisdir=`pwd`
thisdir=`realpath $thisdir`
relocdir=$thisdir/RELOC

./Manifest_Generator.sh DataSM_System_Local_Manifest_Spec > $manifest 2>$pkg_log

echo "Manifest $manifest created"

./Relocation_Collection.sh $manifest $relocdir > $pkg_log 2>&1

echo "Relocation materials collected to $relocdir"

tar cvf DSM_RELOC.tar RELOC > $pkg_log 2>&1

tar --append --file=DSM_RELOC.tar DSM_Deployment_Instructions

gzip DSM_RELOC.tar

echo "Compressed tar file DSM_RELOC.tar.gz Ready for shipment."




