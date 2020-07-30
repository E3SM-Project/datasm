#!/bin/bash

# Usage:  archive_assess_count_first_last.sh archive_map(all or part) [trust_match]
# For the entire archive, use /p/user_pub/e3sm/archive/.cfg/Archive_Map
# For a subset, create a file with selected lines, and give its full path

# if "trust_match" is absent, and the canonical part of the first/last filenames (used to extract sim-dates)
# do NOT match, The output will not calculate "years" for that dataset, and the output will contain lines
# NO_MATCH: <filenames> in order to allow manual inspection.  If the inspection indicates that the files
# belong to different datasets, or are otherwise inappropriately paired, then there are errors in the
# Archive_Map that need to be addressed (the file-matching patterns are insufficiently specific.)
# If the inspection indicates that the match failures are due to "harmless" differences (different
# machine-names, or different generation-dates due to restarts) then re-run this script with
# "trust_match" (as indicated in "Usage" above) to skip the first/last filename check.

PURPOSE="1.  Find and List all Default Model Outputs from all Experiment/Ensembles by Holodeck of each archive index.db file"

# format:  Campaign,Model,Experiment,Ensemble,DatasetType,ArchivePath,DatatypeExtractionPattern,Notes
# Full Archive Map file:  /p/user_pub/e3sm/archive/.cfg/Archive_Map

archive_map_file=$1
simdate_locator=/p/user_pub/e3sm/archive/.cfg/Filename_SimDate_Locator

The_Holodeck=/p/user_pub/e3sm/bartoletti1/Pub_Status/ArchiveAssess/Holodeck


zstash_version=`zstash version`
if [ $zstash_version != "v0.4.1" ]; then
	echo "ABORTING:  zstash version is not 0.4.1 or is unavailable"
	exit 1
fi

trust_match=0
if [ $# -eq 2 ]; then
        if [ $2 == "trust_match" ]; then
                trust_match=1
        fi
fi


startTime=`date +%s`

IFS=$'\n'

cd $The_Holodeck

for aline in `cat $archive_map_file`; do

	apath=`echo $aline | cut -f6 -d,`
	if [ $apath == "NAV" ]; then
		continue
	fi

	basetag=`echo $aline | cut -f1-5 -d,`
	pattern=`echo $aline | cut -f7 -d,`


	rm -rf $The_Holodeck/*
	mkdir $The_Holodeck/zstash

	ln -s $apath/index.db $The_Holodeck/zstash/index.db
	
	zstash ls --hpss=none $pattern > /tmp/zstash_manifest

	filecount=`cat /tmp/zstash_manifest | wc -l`
	if [ $filecount -eq 0 ]; then
		echo "$basetag,0,(pattern=$pattern)"
		continue
	fi

	# SHOULD sort on simdate field here. Not easy, because the
	# tarpath could make the indexing difficult.  Or not. 
	init_file=`cat /tmp/zstash_manifest | head -1`
	last_file=`cat /tmp/zstash_manifest | tail -1`

	# trim tar-path from first and last
	filebase1=`echo $init_file | rev | cut -f1 -d/ | rev`
	filebase2=`echo $last_file | rev | cut -f1 -d/ | rev`

	simdfield=`grep $basetag: $simdate_locator | cut -f2 -d:`
	prevfield=$((simdfield - 1))
	
	sim_date1=`echo $filebase1 | cut -f$simdfield -d.`
	filetest1=`echo $filebase1 | cut -f1-$prevfield -d.`
	sim_date2=`echo $filebase2 | cut -f$simdfield -d.`
	filetest2=`echo $filebase2 | cut -f1-$prevfield -d.`

	# last part eliminates variable leading run_dates
	filecore1=`echo $filetest1 | cut -f2- -d.`
	filecore2=`echo $filetest2 | cut -f2- -d.`

	if [ $trust_match -eq 0 ]; then
		if [ $filecore1 != $filecore2 ]; then
			echo "$basetag,NO_MATCH,$filecount,$init_file,$last_file"
			echo "NO_MATCH:               $filecore1"
			echo "NO_MATCH:               $filecore2"
			continue
		fi
	fi

	y1=`echo $sim_date1 | cut -c1-4`
	y2=`echo $sim_date2 | cut -c1-4`
	m2=`echo $sim_date2 | cut -c6-7`
	y1=$((10#$y1))
	y2=$((10#$y2))
	yspan=$((y2 - y1 + 1))
	if [ $m2 == "01" ]; then
		yspan=$((y2 - y1))
	fi
	echo "$basetag,MATCHED,$filecount,$filetest1,$sim_date1,$sim_date2,$yspan"

done

finalTime=`date +%s`

et=$(($finalTime-$startTime))

echo "Done.  ET = $et"


exit

