#!/bin/bash

# USAGE:  extract_dataset_files_to.sh  file_with_one_line_from_archive_map [dest_dir]
#
# This script simply accepts a file with a line from the Archive map and a path to a destination directory, and 
# populates that directory with the indicated dataset files.  The destination directory must already exist.
# if no "dest_dir" is supplied, only the list of files that would have been extracted is produced
#
# The Holodeck will remain for further manual operations, in either case.
#
# NOTE: As this script will "cd" to the Holodeck, you should supply full paths to the files and directories,
# or else execute from the Holodeck with Holodeck-relative paths

spec=`cat $1 | head -1`
dest=$2

list_only=1

if [ $# -gt 1 ]; then
	dest=$2
	list_only=0

	if [ ! -d $dest ]; then
		echo "ERROR:  Expected destination directory not found: $dest"
		exit 1;
	fi
fi

# Relocatable paths
arch_cfg=`$DSM_GETPATH ARCHIVE_MANAGEMENT`
toolpath=`$DSM_GETPATH STAGING_TOOLS`

Archive_Map=$arch_cfg/Archive_Map


zstash_version=`zstash version`
zstash_version_major=${zstash_version:0:2}

if [ $zstash_version_major == "v0" ]; then
	echo "ABORTING:  zstash version is not v1.0.0 or greater, or is unavailable"
	exit 1
fi

user=`whoami`
userpath=`$DSM_GETPATH USER_ROOT`/$user

extraction_service=$toolpath/archive_extraction_service.py
mapfilegen_service=$toolpath/mapfile_generation_service.py

if [ ! -d $userpath ]; then
    echo "ERROR: No user path \"$userpath\" found."
    exit 1
fi


IFS=$'\n'

# Campaign,Model,Experiment,Resolution,Ensemble,DatasetType,OutputType,ArchivePath,DatatypeTarExtractionPattern,Notes

campaign=`echo $spec | cut -f1 -d,`
modelver=`echo $spec | cut -f2 -d,`
exp_name=`echo $spec | cut -f3 -d,`
resoluti=`echo $spec | cut -f4 -d,`
ensemble=`echo $spec | cut -f5 -d,`
arch_dir=`echo $spec | cut -f8 -d,`
tar_patt=`echo $spec | cut -f9 -d,`

#OVERRIDE
# tar_patt=${tar_patt}000*

The_Holodeck=$userpath/Operations/3_DatasetExtraction/Holodeck
mkdir -p $The_Holodeck

echo "Cleaning the Holodeck"
echo " "
rm -rf $The_Holodeck/*

zstash_dir=$The_Holodeck/zstash
mkdir $zstash_dir

# create Holodeck symlinks to archive tar-files
for targ in `ls $arch_dir`; do
	# touch nothing
	# echo "ln -s $arch_dir/$targ zstash/$targ"
	ln -s $arch_dir/$targ $zstash_dir/$targ
done

cd $The_Holodeck

if [ $list_only -eq 1 ]; then

	echo "zstash ls --hpss=none $tar_patt"
	zstash ls --hpss=none $tar_patt
	exitcode=$?

	if [ $exitcode -ne 0 ]; then
		echo "ERROR:  zstash returned exitcode $exitcode"
		exit $exitcode
	fi

	exit 0
fi

echo "zstash extract --hpss=none $tar_patt"
zstash extract --hpss=none $tar_patt
exitcode=$?

if [ $exitcode -ne 0 ]; then
	echo "ERROR:  zstash returned exitcode $exitcode"
	exit $exitcode
fi

echo "mv $The_Holodeck/$file_extract_pattern $dest"
mv $The_Holodeck/$tar_patt $dest
chmod 644 $dest/*.nc


echo " "
echo "Thank you for using the Zstash Holodeck(tm)"
echo " "

exit 0

