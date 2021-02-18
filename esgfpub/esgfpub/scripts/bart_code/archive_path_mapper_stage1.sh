#!/bin/bash

# Usage:  archive_path_mapper_stage1.sh achive_locations_file
# For the entire archive, use /p/user_pub/e3sm/archive/.cfg/Archive_Locator
# For a subset, create a file with selected lines, and give its full path

PURPOSE="1.  Find and List all Default Model Outputs from all Experiment/Ensembles by Holodeck of each archive index.db file"

# format:  Campaign,Model,Experiment,Ensemble,ArchivePath,
# Full Archve Location file:  /p/user_pub/e3sm/archive/.cfg/Archive_Locator

archive_loc_file=$1

the_SDEP=/p/user_pub/e3sm/archive/.cfg/Standard_Datatype_Extraction_Patterns

WorkDir=/p/user_pub/e3sm/bartoletti1/Pub_Status/ArchivePathMapper/
The_Holodeck=$WorkDir/Holodeck


zstash_version=`zstash version`
if [ $zstash_version != "v0.4.1" ]; then
	echo "ABORTING:  zstash version is not 0.4.1 or is unavailable"
	exit 1
fi

mv $WorkDir/PathsFound/* $WorkDir/PrevPathsFound/

startTime=`date +%s`

IFS=$'\n'

cd $The_Holodeck

# Archive_Locator Entry processed

ALE=0

for aline in `cat ../$archive_loc_file`; do

	apath=`echo $aline | cut -f5 -d,`
	if [ $apath == "NAV" ]; then
		continue
	fi

	ALE=$(($ALE + 1))

	basetag=`echo $aline | cut -f1-4 -d, | tr , :`

	# echo "ALE:BaseTag = $ALE:$basetag"

	rm -rf $The_Holodeck/*
	mkdir $The_Holodeck/zstash

	ln -s $apath/index.db $The_Holodeck/zstash/index.db
	
	for fpatspec in `cat $the_SDEP`; do
		dstitle=`echo $fpatspec | cut -f1 -d, | tr ' ' _`
		dspattern=`echo $fpatspec | cut -f2 -d,`
		if [ $dspattern == "ignore" ]; then
			continue
		fi
		outfile="$ALE:$basetag:$dstitle"
		# echo "        $outfile"
		zstash ls --hpss=none $dspattern > ../PathsFound/$outfile	
		if [ -s ../PathsFound/$outfile ]; then
			continue
		fi
		echo "EMPTY: $outfile"
		rm ../PathsFound/$outfile
	done

done

finalTime=`date +%s`

et=$(($finalTime-$startTime))

echo "Done.  ET = $et"


exit

