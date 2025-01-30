#!/bin/bash

testval=`realpath "Relocation/dsm_get_root_path.sh"`
if [[ $? -ne 0 ]]; then
    echo "Please execute this script in your (git repo)/datasm/datasm directory."
    exit 0
fi

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <chosen_dsm_staging_location> <updated_dsm_root_paths_file>"
    exit 0
fi

staging=$1
rootpaths=$2

if [[ ! -d $staging ]]; then
    echo "Error:  location \"$staging\" must exist."
    exit 0
fi

if [[ ! -f $rootpaths ]]; then
    echo "Error: cannot locate new root_paths file $rootpaths"
    exit 0
fi

path_mgmt="$staging/Relocation"

mkdir -p $path_mgmt

echo "Copying $rootpaths to $path_mgmt/.dsm_root_paths"
cp $rootpaths $path_mgmt/.dsm_root_paths

echo "Copying dsm_get_root_path.sh to $path_mgmt/.dsm_iget_root_path"
cp Relocation/dsm_get_root_path.sh $path_mgmt/.dsm_get_root_path.sh

chmod 750 $path_mgmt/.dsm_get_root_path.sh




