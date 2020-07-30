#!/bin/bash

infile=archive_dataset_map_prelim

archive_loc=/p/user_pub/e3sm/archive/.cfg/Archive_Locator

for aline in `cat $infile`; do
	pureline=`echo $aline | cut -f2- -d:`
	headpart=`echo $pureline | cut -f1-4 -d:`
	ds_title=`echo $pureline | cut -f5 -d:`
	epattern=`echo $pureline | cut -f6 -d:`

	seek=`echo $headpart | tr : ,`
	found=`cat $archive_loc | grep $seek`

	lc=`echo $found | wc -l`
	if [ $lc -eq 0 ]; then
		echo "ERROR: no archive location found for $seek"
		continue
	fi
	if [ $lc -gt 1 ]; then
		echo "WARNING: multiple archive locations found for $seek"
	fi
	for apath in $found; do
		the_path=`echo $apath | cut -f5 -d,`
		echo "$seek,$ds_title,$the_path,$epattern"
	done
done

	

