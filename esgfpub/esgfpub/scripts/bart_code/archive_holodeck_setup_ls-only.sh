#!/bin/bash

# echo "DEBUG:  argcount = $#"

instructions="full path to a leaf archive directory"

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

	
The_Holodeck=/p/user_pub/e3sm/bartoletti1/Pub_Status/ArchiveAssess/Holodeck

IFS=$'\n'

# PLAN:  populate the subordinate "zstash" subdirectory with simlinks to the appropriate tarfiles and index.db file.

echo "Clearing the Holodeck"
echo " "
rm -rf $The_Holodeck/*

zstash_dir=$The_Holodeck/zstash
mkdir $zstash_dir

echo "We will set up holodeck zstash ls for directory $fulldir" 

ln -s $fulldir/index.db $zstash_dir/index.db

echo "Holodeck prepared for entry. Enjoy your simulation!"

exit

