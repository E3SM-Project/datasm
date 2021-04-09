#!/bin/bash

# Usage:  process_sproket_output.sh  E3SM_datafile_urls-<timestamp>

sproketdir=/p/user_pub/e3sm/bartoletti1/Pub_Status/sproket/

datafile_urls=`ls $sproketdir | grep E3SM_datafile_urls- | tail -1`


ts=`date +%Y%m%d.%H%M%S`
dsid_file_paths="E3SM_dsid_file_paths-$ts"
dsid_leaf_paths="E3SM_dsid_leaf_paths-$ts"
publication_dsids="E3SM_dataset_ids-$ts"
publication_report="ESGF_publication_report-$ts"

cat $datafile_urls | cut -f7- -d/ | sort | uniq > $dsid_file_paths
cat $dsid_file_paths | rev | cut -f2- -d/ | rev | sort | uniq > $dsid_leaf_paths


for aline in `cat $dsid_leaf_paths`; do
	filecount=`grep $aline $dsid_file_paths | wc -l`
	firstfile=`grep $aline $dsid_file_paths | rev | cut -f1 -d/ | rev | sort | uniq | head -1`
	finalfile=`grep $aline $dsid_file_paths | rev | cut -f1 -d/ | rev | sort | uniq | tail -1`
	datasetID=`echo $aline | tr / .`

	# handle variable sim-date year counts

	if [[ $firstfile =~ -*([0-9]{4}-[0-9]{2}) ]]; then

		# process YYYY-MM
		yearmo1=`echo ${BASH_REMATCH[1]}`
		if [[ $finalfile =~ -*([0-9]{4}-[0-9]{2}) ]]; then
			yearmo2=`echo ${BASH_REMATCH[1]}`
		else
			echo "NOMATCH,$filecount,$datasetID,$firstfile" >> $publication_report
			continue
		fi

		y1=`echo $yearmo1 | cut -c1-4`
		y2=`echo $yearmo2 | cut -c1-4`
		m2=`echo $yearmo2 | cut -c6-7`

		if [ $m2 == "01" ]; then
			yspan=$((10#$y2 - 10#$y1))
		else
			yspan=$((10#$y2 - 10#$y1 + 1))
		fi

		echo "$yspan,$filecount,$datasetID,$firstfile" >> $publication_report
		

	elif [[ $firstfile =~ -*([0-9]{6}_[0-9]{6}) ]]; then

		# process YYYYMM-YYYYMM first file only
		ym_ym=`echo ${BASH_REMATCH[1]}`

		y1=`echo $ym_ym | cut -c1-4`
		y2=`echo $ym_ym | cut -c8-11`
		m2=`echo $ym_ym | cut -c12-13`

		if [ $m2 == "01" ]; then
			yspan=$((10#$y2 - 10#$y1))
		else
			yspan=$((10#$y2 - 10#$y1 + 1))
		fi

		echo "$yspan,$filecount,$datasetID,$firstfile" >> $publication_report
	else
		echo "NOMATCH,$filecount,$datasetID,$firstfile" >> $publication_report
		continue
	fi

done


