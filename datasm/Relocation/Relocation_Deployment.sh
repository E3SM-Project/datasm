#!/bin/bash

#  This script must be given the full path to the untarred "RELOC"
#  DSM Packacge directory, if it is not found directly below the current
#  directory.
#
#       ./Relocation_Deployment.sh [<full_path_to_RELOC>]
#
#  It is assumed that the dsm_root_paths file
#
#       RELOC/DSM_STAGING/.dsm_root_paths
#
#  has been edited to according to the DSM_Deployment_Instructions to
#  provide the desired deployment paths for the contained root_tag values.
#  These paths must either exist, or can be created by running this script.
#
#  The user must have group rwx permissions to the intended directories.
#
#  The contents of the opened "RELOC" directory elements will be moved
#  to their corresponding destinations.  Additionally, the script
#
#       RELOC/DSM_STAGING/.dsm_get_root_path.sh
#
#  will be edited to install the path to the .dsm_root_paths file.

reloc_dir=""

if [ $# -eq 0 ]; then
    pdir=`pwd`
    pdir=`realpath $pdir`
    rloc=$pdir/RELOC
    if [ -d $rloc ]; then
        reloc_dir=$rloc
    else
        echo "ERROR: Current directory does not contain the DSM Package \"RELOC\" directory."
        echo "You may use \"./Relocation_Deployment.sh [<full_path_to_RELOC>]\" as well."
        exit 1
    fi
fi

if [ $# -eq 1 ]; then
    rloc=$1
    tdir=`basename $rloc`
    if [ $tdir != "RELOC" ]; then
        echo "ERROR: provided path does not include the DSM Package \"RELOC\" directory."
        echo "You may also simply execute this script above the \"RELOC\" directory."
        exit 1
    fi
    if [ -d $rloc ]; then
        reloc_dir=$rloc
    else
        echo "ERROR: Provided path $rloc does not exist."
        echo "You may use \"./Relocation_Deployment.sh [<full_path_to_RELOC>]\"."
        echo "You may also simply execute this script above the \"RELOC\" directory."
        exit 1
    fi
fi

# Test that the root_paths_file is usable

root_paths_file=$reloc_dir/DSM_STAGING/Relocation/.dsm_root_paths

if [ ! -f $root_paths_file ]; then
    echo "ERROR: Cannot locate root_paths file: $root_paths_file"
    echo "  This file must contain \"<Root_Tag>:<full_path>\" entries for every"
    echo "  RELOC/<RootTag> listed under the RELOC directory."
    exit 1
fi

unsetcount=`grep SAMPLE $root_paths_file | wc -l`
if [ $unsetcount -gt 0 ]; then
    echo "ERROR: The root_paths file: $root_paths_file"
    echo "  has not been properly edited.  Please replace all <RootTag>:[SAMPLE]<path>"
    echo "  entries with <RootTag>:<full_path_to_destination> entries."
    exit 1
fi

# Test that intended destinations are accessible

this_user=`whoami`

for aline in `cat $root_paths_file`; do
    rootTag=`echo $aline | cut -f1 -d:`
    rootVal=`echo $aline | cut -f2 -d:`

    if [ $rootTag == "USER_ROOT" ]; then
        rootVal=$rootVal/$this_user
    fi

    mkdir -p $rootVal 2>/dev/null
    rv=$?
    if [ $rv -ne 0 ]; then
        echo "ERROR:  (RootTag = $rootTag): Cannot find or create destination directory $rootVal"
        exit 1
    fi
done

# Capture the root_paths_file locally, for later reference post-deployment

cp $root_paths_file .RPF

# Proceeding to Deployment

total_deploy_count=0

for aline in `cat .RPF`; do
    rootTag=`echo $aline | cut -f1 -d:`
    rootVal=`echo $aline | cut -f2 -d:`

    if [ $rootTag == "USER_ROOT" ]; then
        rootVal=$rootVal/$this_user
    fi

    srcdir=$reloc_dir/$rootTag

    echo ""
    echo "Processing srcdir $srcdir to dest $rootVal"

    depcount=0
    for item in `ls -a $srcdir`; do
        if [[ $item == "." || $item == ".." ]]; then
            continue
        fi
        mv $srcdir/$item $rootVal
        depcount=$((depcount + 1))
    done
    volume=`du -b $reloc_dir/$rootTag | tail -1 | cut -f1 -d' '`
    echo "$rootTag:     Deployed $depcount items        ($volume Bytes)"
    total_deploy_count=$((total_deploy_count + depcount))
done

echo ""
echo Total items deployed: $total_deploy_count
echo ""

# Finally:  Setup Path Relocation System ...

new_dsm_stp=`grep DSM_STAGING .RPF | cut -f2 -d:`
reloc_home=$new_dsm_stp/Relocation

sed -i "s%RELOC_HOME%$reloc_home%" $reloc_home/.dsm_get_root_path.sh


echo "DataSM System Deployment Completed. Intended user/operators will need to add
the following export to their personal .bashrc file:

    export DSM_GETPATH=$reloc_home/.dsm_get_root_path.sh

Enjoy!"


