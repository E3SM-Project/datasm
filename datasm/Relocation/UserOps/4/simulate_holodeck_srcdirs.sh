#!/bin/bash

wh_dirlist=$1
holodeck=$2

holomake=/p/user_pub/e3sm/bartoletti1/acode/create_holodeck_from_dsid.sh

for adir in `cat $wh_dirlist`; do
    dsid=`echo $adir | cut -f6- -d/ | rev | cut -f2- -d/ | rev | tr / .`
    # echo "$holomake $holodeck $dsid $adir"
    $holomake $holodeck $dsid $adir
done
