#!/bin/bash

# This routine uses the "DataSM Relocation Manifest" to locate and collect
# all UserOp support scripts and configs to the git repository for datasm
# (under datasm/Relocation/UserOps).  The manifest is produced by running
# "Manifest_Generate.sh DataSM_System_Local_Manifest_Spec", both of which
# are located in datasm/Relocation.

manifest=$1

git_repo_userops=/home/bartoletti1/gitrepo/datasm/datasm/Relocation/UserOps

flist1=`cat $manifest | grep USEROP | grep FILE | grep -v PATHTO`

for aline in $flist1; do
    part1=`echo $aline | cut -f4 -d,`
    part2=`echo $aline | cut -f5 -d,`
    srcpath=$part1/$part2
    dirtag=`basename $part1 | cut -f1 -d_`

    echo "cp $srcpath $git_repo_userops/$dirtag"
    cp $srcpath $git_repo_userops/$dirtag

done


