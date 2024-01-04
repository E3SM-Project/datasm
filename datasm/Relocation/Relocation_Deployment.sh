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


root_paths_file=$1

rpf_base=`basename $root_paths_file`
rpf_dir=`dirname $root_paths_file`

if [ $rpf_base != ".dsm_root_paths" ]; then
    echo "ERROR:  Supplied root_paths file ($root_paths_file) does not end with \".dsm_root_paths\""
    exit 1
fi

if [ ! -f $root_paths_file ]; then
    echo "ERROR:  Cannot locate root_paths file ($root_paths_file)"
    exit 1
fi

# Proceeding to Deployment

# part 1: test that intended destinations are accessible

for aline in `cat $rootPpaths_file`; do
    rootTag=`echo $aline | cut -f1 -d:`
    rootVal=`echo $aline | cut -f2 -d:`

    mkdir -p $rootVal 2>/dev/null
    rv=$?
    if [ $rv -ne 0 ]; then
        echo "ERROR:  Cannot find or create destination directory $rootVal (for $rootTag)"
        exit 1
    fi
done

for aline in `cat $rootPpaths_file`; do
    rootTag=`echo $aline | cut -f1 -d:`
    rootVal=`echo $aline | cut -f2 -d:`

    srcdir=
    


# Eventually ...

new_dsm_stp=`grep DSM_STAGING $root_paths_file | cut -f2 -d:`

sed -i "s%RELOC_HOME%$new_dsm_stp" $rpf_dir/.dsm_get_root_path.sh


echo "DataSM System Deployment Completed. Intended user/operators will need to add
the following export to their personal .bashrc file:

    export DSM_GETPATH=$new_dsm_stp/.dsm_get_root_path.sh

Enjoy!"


