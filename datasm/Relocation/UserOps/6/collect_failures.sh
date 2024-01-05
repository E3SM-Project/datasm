#!/bin/bash

for odir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6; do
    cd $odir
    grep "is in state" Publication_Log-* | grep Fail | cut -f1 -d: | cut -f3- -d- | sort | uniq > Failures_$odir
    cd ..
done
