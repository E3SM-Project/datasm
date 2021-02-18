#!/bin/bash

# 
pathhead=$1

catalogeur=/p/user_pub/e3sm/bartoletti1/Pub_Status/catalog_E3SM_PUB.sh

pretag=`date +%s`

for ens in ens1 ens2 ens3 ens4 ens5; do
	for v in v1 v2 v3; do
		$catalogeur $pathhead $ens.$v $pretag
	done
done

genlist=`ls | grep $pretag`
onefile=`ls | grep $pretag | head -1`

newtag=`echo $onefile | cut -f2-3 -d-`
now_utc=`date -u +%Y%m%d.%H%M`
count_pathlist_v="$newtag-$now_utc"

cat ${pretag}* | sort | uniq > $count_pathlist_v

rm ${pretag}*


# Now for part 2:

inputform="count,pubpath"
datasetID="Project.ModelVer.Experiment.Resolution[.F2010_hi_low].Realm.Grid.Outtype.Freq.Ens.vn"

now_utc=`date -u +%Y%m%d.%H%M`

outfile=task-stat-list3-$now_utc

echo "ModVer,Resolution,Ens,Title,Status,Grid,Years,Filecount,Dataset_ID,Dataset_First_File,Dataset_Publication_Path,Notes,Title_Expanded" > $outfile

for aline in `cat $count_pathlist_v`; do

	fcount=`echo $aline | cut -f1 -d,`
	apath=`echo $aline | cut -f2 -d,`
	# avers=`echo $aline | cut -f3 -d,`

	ffile=`ls $apath | head -1`

	# NOTE: requires (pub) env to access ncdump
	# tperiod=`ncdump -h $apath/$ffile | grep "time_period_freq" | cut -f2 -d=`

	dsid=`echo $apath | tr / . | cut -f5- -d.`

	modver=`echo $dsid | cut -f2 -d.`
	experiment=`echo $dsid | cut -f3 -d.`

	if [[ $experiment =~ "F2010" ]]; then
		resstr=`echo $dsid | cut -f4 -d.`
		realm=`echo $dsid | cut -f6 -d.`
		grid=`echo $dsid | cut -f7 -d.`
		outtype=`echo $dsid | cut -f8 -d.`
		freq=`echo $dsid | cut -f9 -d.`
		ens=`echo $dsid | cut -f10 -d.`
	else
		resstr=`echo $dsid | cut -f4 -d.`
		realm=`echo $dsid | cut -f5 -d.`
		grid=`echo $dsid | cut -f6 -d.`
		outtype=`echo $dsid | cut -f7 -d.`
		freq=`echo $dsid | cut -f8 -d.`
		ens=`echo $dsid | cut -f9 -d.`
	fi


	if [ $realm == "atmos" ]; then
		realmcode="atm"
	elif [ $realm == "land" ]; then
		realmcode="lnd"
	elif [ $realm == "ocean" ]; then
		realmcode="ocn"
	elif [ $realm == "sea-ice" ]; then
		realmcode="sice"
	else	realmcode="misc"
	fi

	gridcode="nat"
	if [ $grid != "native" ]; then
		gridcode="reg"
	fi

	title="$realmcode $gridcode $freq"



	echo "$modver,$resstr,$ens,$title,published,$grid,,$fcount,$dsid,$ffile,$apath,$outtype," >> $outfile

done


