#!/bin/bash

# Report individual and total volume of local E3SM publication directories.
# Individuals are by ModelVersion/Experiment

ts=`date -u +%Y%m%d`

pub_root=`$DSM_GETPATH PUBLICATION_DATA`

tmp_report="tmp_E3SM_vol_report-$ts"

for moddir in `ls $pub_root/E3SM`; do
    dlist=`ls $pub_root/E3SM/$moddir`
    for subdir in $dlist; do
        # echo $moddir,$subdir >> $tmp_report
        # continue
        vol1=`du -b $pub_root/E3SM/$moddir/$subdir | tail -1 | cut -f1`
        volTB=$(echo "scale=2; $vol1/1000000000000" | bc)
        echo $moddir/$subdir,$volTB >> $tmp_report
    done
done

SummV=0.0

for aval in `cat $tmp_report | cut -f3 -d,`; do
    SummV=`echo "$SummV + $aval" | bc`
done

report="E3SM_Publication_Volume_by_Experiment-${ts}.csv"

echo "" > $report
echo "E3SM_Publication_Volume_by_Experiment-${ts}" >> $report
echo "" >> $report

echo "ModelVersion,Experiment,Volume_TB" >> $report

cat $tmp_report >> $report
rm -f $tmp_report

echo "" >> $report
echo ",TotalVolume,$SummV" >> $report

echo "E3SM volume report ($report) completed."
