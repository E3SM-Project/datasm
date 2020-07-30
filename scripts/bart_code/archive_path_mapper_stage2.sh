#!/bin/bash

# for each named file in PathsFound

flist=`ls PathsFound`

rm -f headset_list_first_last

for afile in `ls PathsFound`; do
	echo $afile >> headset12_lists_first_last
	# the list of grep -v exclusions here may need future additions
	headlist=`cat PathsFound/$afile | cut -c1-20 | sort | uniq | grep -v "^rest/" | grep -v "^post/" | grep -v "^test" | grep -v "^init" | grep -v "^run/try" | grep -v "^run/bench" | grep -v "^old/run" | grep -v "^pp/remap" | grep -v "^a-prime" | grep -v "^lnd_rerun" | grep -v "^atm/ncdiff" | grep -v "^archive/rest" | grep -v "fullD" | grep -v "photic"`
	for ahead in $headlist; do
		samp=`grep ^$ahead PathsFound/$afile | sort | head -1`
		echo "    HEADF:$samp" >> headset_list_first_last
		samp=`grep ^$ahead PathsFound/$afile | sort | tail -1`
		echo "    HEADL:$samp" >> headset_list_first_last
	done
done
