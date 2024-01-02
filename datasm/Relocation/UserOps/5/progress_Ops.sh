#!/bin/bash

ts=`date -u +%Y%m%d_%H%M%S_%6N`

tmpfile1=temp_${ts}.1
tmpfile2=temp_${ts}.2

for opdir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6 Ops7 Ops8 Ops9; do
    rm -f $tmpfile1
    echo "(Processing Opdir $opdir)"
    for afile in `ls $opdir/PostProcess_Log*`; do
        grep "is in state" $afile | egrep "POSTPROCESS:Fail|POSTPROCESS:Pass" | tail -1 >> $tmpfile1 2> /dev/null
    done
    for aline in `cat $tmpfile1`; do
        echo $opdir:$aline >> $tmpfile2
    done
done

rm -f $tmpfile1

count1=`grep Ops1 $tmpfile2 | wc -l`
count2=`grep Ops2 $tmpfile2 | wc -l`
count3=`grep Ops3 $tmpfile2 | wc -l`
count4=`grep Ops4 $tmpfile2 | wc -l`
count5=`grep Ops5 $tmpfile2 | wc -l`
count6=`grep Ops6 $tmpfile2 | wc -l`
count7=`grep Ops7 $tmpfile2 | wc -l`
count8=`grep Ops8 $tmpfile2 | wc -l`
count9=`grep Ops9 $tmpfile2 | wc -l`

targ1=`ls Ops1/target* | cut -f2 -d/`
targ2=`ls Ops2/target* | cut -f2 -d/`
targ3=`ls Ops3/target* | cut -f2 -d/`
targ4=`ls Ops4/target* | cut -f2 -d/`
targ5=`ls Ops5/target* | cut -f2 -d/`
targ6=`ls Ops6/target* | cut -f2 -d/`
targ7=`ls Ops7/target* | cut -f2 -d/`
targ8=`ls Ops8/target* | cut -f2 -d/`
targ9=`ls Ops9/target* | cut -f2 -d/`

targ1v=`echo $targ1 | cut -f2 -d-`
targ2v=`echo $targ2 | cut -f2 -d-`
targ3v=`echo $targ3 | cut -f2 -d-`
targ4v=`echo $targ4 | cut -f2 -d-`
targ5v=`echo $targ5 | cut -f2 -d-`
targ6v=`echo $targ6 | cut -f2 -d-`
targ7v=`echo $targ7 | cut -f2 -d-`
targ8v=`echo $targ8 | cut -f2 -d-`
targ9v=`echo $targ9 | cut -f2 -d-`


now=`date -u +%Y%m%d_%H%M%S`

echo "" >> The_Trace
echo "$now:" >> The_Trace

count1a=`grep Ops1 $tmpfile2 | grep Pass | wc -l`
count1b=`grep Ops1 $tmpfile2 | grep Fail | wc -l`
remain1=$((targ1v - count1a - count1b))
echo "Ops1: ($targ1) $count1a Pass, $count1b Fail, $remain1 Remain " >> The_Trace

count2a=`grep Ops2 $tmpfile2 | grep Pass | wc -l`
count2b=`grep Ops2 $tmpfile2 | grep Fail | wc -l`
remain2=$((targ2v - count2a - count2b))
echo "Ops2: ($targ2) $count2a Pass, $count2b Fail, $remain2 Remain " >> The_Trace

count3a=`grep Ops3 $tmpfile2 | grep Pass | wc -l`
count3b=`grep Ops3 $tmpfile2 | grep Fail | wc -l`
remain3=$((targ3v - count3a - count3b))
echo "Ops3: ($targ3) $count3a Pass, $count3b Fail, $remain3 Remain " >> The_Trace

count4a=`grep Ops4 $tmpfile2 | grep Pass | wc -l`
count4b=`grep Ops4 $tmpfile2 | grep Fail | wc -l`
remain4=$((targ4v - count4a - count4b))
echo "Ops4: ($targ4) $count4a Pass, $count4b Fail, $remain4 Remain " >> The_Trace

count5a=`grep Ops5 $tmpfile2 | grep Pass | wc -l`
count5b=`grep Ops5 $tmpfile2 | grep Fail | wc -l`
remain5=$((targ5v - count5a - count5b))
echo "Ops5: ($targ5) $count5a Pass, $count5b Fail, $remain5 Remain " >> The_Trace

count6a=`grep Ops6 $tmpfile2 | grep Pass | wc -l`
count6b=`grep Ops6 $tmpfile2 | grep Fail | wc -l`
remain6=$((targ6v - count6a - count6b))
echo "Ops6: ($targ6) $count6a Pass, $count6b Fail, $remain6 Remain " >> The_Trace

count7a=`grep Ops7 $tmpfile2 | grep Pass | wc -l`
count7b=`grep Ops7 $tmpfile2 | grep Fail | wc -l`
remain7=$((targ7v - count7a - count7b))
echo "Ops7: ($targ7) $count7a Pass, $count7b Fail, $remain7 Remain " >> The_Trace

count8a=`grep Ops8 $tmpfile2 | grep Pass | wc -l`
count8b=`grep Ops8 $tmpfile2 | grep Fail | wc -l`
remain8=$((targ8v - count8a - count8b))
echo "Ops8: ($targ8) $count8a Pass, $count8b Fail, $remain8 Remain " >> The_Trace

count9a=`grep Ops9 $tmpfile2 | grep Pass | wc -l`
count9b=`grep Ops9 $tmpfile2 | grep Fail | wc -l`
remain9=$((targ9v - count9a - count9b))
echo "Ops9: ($targ9) $count9a Pass, $count9b Fail, $remain9 Remain " >> The_Trace

rm -f $tmpfile2

tot_pass=$((count1a + count2a + count3a + count4a + count5a + count6a + count7a + count8a + count9a ))
tot_fail=$((count1b + count2b + count3b + count4b + count5b + count6b + count7b + count8b + count9b ))
tot_left=$((remain1 + remain2 + remain3 + remain4 + remain5 + remain6 + remain7 + remain8 + remain9 ))
echo "Total_Pass $tot_pass, Total_Fail $tot_fail, Total_Remaining $tot_left" >> The_Trace

tail -30 The_Trace

echo ""
echo "Is anyone stuck?"
ls -l Ops*/slurm_scripts/*.out


