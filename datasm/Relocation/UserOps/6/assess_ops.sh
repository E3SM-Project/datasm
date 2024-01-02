#!/bin/bash

self=`whoami`
opsdir=`$DSM_GETPATH USER_ROOT`/$self/Operations/6_DatasetPublication

tottarg=0
totpass=0

for odir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6; do

    targcount=`ls $opsdir/$odir/target* | cut -f2 -d-`
    passcount=`grep "is in state" $opsdir/$odir/Publication* | grep Pass | wc -l`
    leftcount=$((targcount - passcount))

    pct=$(echo "scale=3; $passcount/$targcount" | bc)

    echo "$odir:  target=$targcount:  Pass=$passcount (pct=$pct):  remain=$leftcount"

    tottarg=$((tottarg + targcount))
    totpass=$((totpass + passcount))

done

    PCT=$(echo "scale=3; $totpass/$tottarg" | bc)
echo ""
echo "target = $tottarg:  Pass = $totpass:  (PCT = $PCT)"
