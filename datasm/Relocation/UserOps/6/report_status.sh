#!/bin/bash

odir=`pwd`
odir=`basename $odir`

grep "is in state" Publication_Log-* | grep Fail | cut -f1 -d: | cut -f3- -d- | sort | uniq > Process_Failures_$odir
failcount=`cat Process_Failures_$odir | wc -l`
mv Process_Failures_$odir Process_Failures_$odir-$failcount

grep "is in state" Publication_Log-* | grep Pass | cut -f1 -d: | cut -f3- -d- | sort | uniq > Process_Successes_$odir
succcount=`cat Process_Successes_$odir | wc -l`
mv Process_Successes_$odir Process_Successes_$odir-$succcount


