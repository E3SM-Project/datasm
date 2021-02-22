#!/bin/bash

# echo "DEBUG:  argcount = $#"

#  USAGE:  archive_holodeck_setup.sh full_path_to_a_leaf_archive_directory"

# EDIT THIS LINE for your own Holodeck
The_Holodeck=/p/user_pub/e3sm/bartoletti1/Pub_Status/ArchiveAssess/Holodeck

zstash_version=`zstash version`
if [ $zstash_version != "v0.4.1" ]; then
	echo "ABORTING:  zstash version is not 0.4.1 or is unavailable"
	exit 1
fi

fulldir=$1

if [ ! -d $fulldir ]; then
	echo "ABORTING:  cannot find archive directory: $fulldir"
	exit 1
fi

IFS=$'\n'

# PLAN:  populate the subordinate "zstash" subdirectory with simlinks to the appropriate tarfiles and index.db file.

echo "Clearing the Holodeck"
echo " "
rm -rf $The_Holodeck/*

zstash_dir=$The_Holodeck/zstash
mkdir $zstash_dir

echo "We will setup holodeck zstash for directory $fulldir" 

# major kludge
rm -rf $The_Holodeck/archive

for targ in `ls $fulldir`; do
	# touch nothing
	# echo "ln -s $fulldir/$targ zstash/$targ"
	ln -s $fulldir/$targ $zstash_dir/$targ
done

echo "Holodeck prepared for entry. Enjoy your simulation!"

exit

