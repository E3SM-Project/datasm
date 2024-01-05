#!/bin/bash

ts=`date -u +%Y%m%d_%H%M%S_%6N`

for opdir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6 Ops7 Ops8 Ops9; do
    # echo "(Processing Opdir $opdir)"
    for afile in `ls $opdir/PostProcess_Log*`; do
        pass_dsid=`grep "is in state" $afile | egrep "POSTPROCESS:Pass" | tail -1 | cut -f4 -d' '`
        echo $pass_dsid
    done
done

