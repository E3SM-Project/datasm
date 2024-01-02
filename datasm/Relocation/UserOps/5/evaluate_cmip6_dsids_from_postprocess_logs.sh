#!/bin/bash

for opdir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6 Ops7 Ops8 Ops9; do

    pplogs=`ls $opdir | grep PostProcess_Log`

    thePass="check_${opdir}_Pass"
    theFail_1="check_${opdir}_Fail_1_JobStart"
    theFail_2="check_${opdir}_Fail_2_JobProcess"
    rm -f $thePass $theFail_1 $theFail_2

    for alog in $pplogs; do
        dsid=`echo $alog | cut -f3- -d-`
        # echo $dsid

        astate=`cat $opdir/$alog | grep "Postprocessing complete"`
        len1=${#astate}

        if [ $len1 -eq 0 ]; then
            echo "$dsid" >> $theFail_1
            continue
        fi

        ptest=`echo $astate | grep Pass`
        len2=${#ptest}

        if [ $len2 -eq 0 ]; then
            echo "$dsid" >> $theFail_2
            continue
        fi
        echo "$dsid" >> $thePass
    done

    # Augment Pass and Fails with known quantities
    cp $thePass btempfile
    cat btempfile $opdir/success_v1_LE_${opdir}_CMIP6_180 | sort | uniq > $thePass
    grep CFmon $opdir/to_generate_v1_LE_${opdir}_CMIP6_dsids_452 >> $theFail_1
    grep Amon.o3 $opdir/to_generate_v1_LE_${opdir}_CMIP6_dsids_452 >> $theFail_2

    mv $theFail_1 atempfile
    cat atempfile | sort | uniq > $theFail_1
    mv $theFail_2 atempfile
    cat atempfile | sort | uniq > $theFail_2
    rm atempfile btempfile

done

dspilc=`$DSM_GETPATH STAGING_TOOLS`/ds_paths_info_dsid_list_compact.sh
trisect=`$DSM_GETPATH STAGING_TOOLS`/trisect.py

# Some "Pass" are silent Fails.  Must record them as Fail_3
for opdir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6 Ops7 Ops8 Ops9; do

    thePass="check_${opdir}_Pass"
    theFail_3="check_${opdir}_Fail_3_Pass_NO_RESULTS"
    rm -f $theFail_3

    badPass=`$dspilc $thePass | grep WH_PATH | grep NO_RESULTS | cut -f2 -d' ' | cut -f1 -d: | cut -f6- -d/ | tr / .`
    for dsid in $badPass; do
        echo "$dsid" >> $theFail_3
    done 

done

# At this point, the "check_Ops_Pass" may include "NO_RESULTS" failures.
# We must select the "Fail_PASS_NO_RESULTS" failures and remove them from "ASSUMED_Pass" to create TRUE_Pass

for opdir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6 Ops7 Ops8 Ops9; do

    cat check_${opdir}_Fail* | sort | uniq > ALL_${opdir}_Fail
    cat check_${opdir}_Pass  > ASSUMED_${opdir}_Pass
    python $trisect ASSUMED_${opdir}_Pass ALL_${opdir}_Fail > junk 2>&1
    mv only-ASSUMED_${opdir}_Pass TRUE_${opdir}_Pass
    rm only* both* junk

done


