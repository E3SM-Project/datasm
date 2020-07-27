#!/bin/bash

arc_list=$1
pub_list=$2

trisect=/p/user_pub/e3sm/bartoletti1/abin/trisect

# using
#   A.  the ArchiveAssess output from "archive_assess_count_first_last.sh"
#   B.  the assessment report from sproket
#   (properly limited to a single common experiment)
#   do
#	1.  Extract "experiment/ensemble and datasetType" from each
#	2.  Use "trisect" to find the "A-only", "B-only", and Both.

# arc_list:  Campaign,Model,Experiment,Ensemble,r_g_f,MATCHED,files,canonFile,beginDate,finalDate,years
# pub_list:  years,files,datasetID,firstFile
#
#   where datasetID = Project.Model.Experiment.Resolution.Realm.Grid.outType.Freq.Ensemble.version

# So . . . Generate A_Key and B_Key lists

rm -f /tmp/A_list
rm -f /tmp/B_list

for aline in `cat $arc_list`; do

	A_model=`echo $aline | cut -f2 -d,`
	A_Exper=`echo $aline | cut -f3 -d,`
	A_Ensem=`echo $aline | cut -f4 -d,`
	A_r_g_f=`echo $aline | cut -f5 -d,`

	echo $A_model,$A_Exper,$A_Ensem,$A_r_g_f >> /tmp/A_list
done

for aline in `cat $pub_list`; do

	ds_ID=`echo $aline | cut -f3 -d,`
	B_model=`echo $ds_ID | cut -f2 -d.`
	B_exper=`echo $ds_ID | cut -f3 -d.`
	B_ensem=`echo $ds_ID | cut -f9 -d.`

	realm=`echo $ds_ID | cut -f5 -d.`
	gridv=`echo $ds_ID | cut -f6 -d.`
	freqv=`echo $ds_ID | cut -f8 -d.`

	if [ $realm == "atmos" ]; then
		r_code="atm"
	elif [ $realm == "land" ]; then
		r_code="lnd"
	elif [ $realm == "ocean" ]; then
		r_code="ocn"
	else
		r_code=$realm
	fi

	if [ $gridv == "native" ]; then
		g_code="nat"
	else
		g_code="reg"
	fi

	B_r_g_f="${r_code}_${g_code}_$freqv"

	echo $B_model,$B_exper,$B_ensem,$B_r_g_f >> /tmp/B_list
done

sort /tmp/A_list | uniq > /tmp/sA_list	
sort /tmp/B_list | uniq > /tmp/sB_list	

$trisect -f1 /tmp/sA_list -f2 /tmp/sB_list

echo "ARCHIVE_ONLY:"
cat f1-only
echo " "
echo "ESGF_PUBLISH_ONLY:"
cat f2-only
echo " "
echo "In ARCHIVE and ESGF_PUBLISH"
cat fboth

exit 0
