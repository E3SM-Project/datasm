#!/bin/bash

ts=`date -u +%Y%m%d`

tmp_report="tmp_CMIP6_vol_report-$ts"

for actv in `ls /p/user_pub/work/CMIP6`; do
    insti=`ls /p/user_pub/work/CMIP6/$actv`
    for inst in $insti; do
        srclist=`ls /p/user_pub/work/CMIP6/$actv/$inst`
        for asrc in $srclist; do
            if [ ${asrc:0:4} != "E3SM" ]; then
                continue
            fi
            elist=`ls /p/user_pub/work/CMIP6/$actv/$inst/$asrc`
            for exper in $elist; do
                # echo "$actv,$inst,$asrc,$exper" >> $tmp_report
                # continue
                vol1=`du -b /p/user_pub/work/CMIP6/$actv/$inst/$asrc/$exper | tail -1 | cut -f1`
                volTB=$(echo "scale=2; $vol1/1000000000000" | bc)
                echo "$actv,$inst,$asrc,$exper,$volTB" >> $tmp_report
            done
        done
    done
done

SummV=0.0

for aval in `cat $tmp_report | cut -f5 -d,`; do
    SummV=`echo "$SummV + $aval" | bc`
done

report="CMIP6_Publication_Volume_by_Experiment-${ts}.csv"

echo "" > $report
echo "CMIP6_Publication_Volume_by_Experiment-${ts}" >> $report
echo "" >> $report

echo "Activity,Institution_ID,Source_ID,Experiment,Volume_TB" >> $report

cat $tmp_report >> $report
rm -f $tmp_report

echo "" >> $report
echo ",,,TotalVolume,$SummV" >> $report

echo "CMIP6 volume report ($report) completed."
