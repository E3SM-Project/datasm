#!/bin/bash

# PRE-RUN indicate target dataset_id, and critical user_metadata values:
# POST-RUN list content of logs, output dirs, etc.


lockdir=$1
case_id=$2
cmip_dsid=$3
dsversion=$4

opdir=`pwd`

resource=`$DSM_GETPATH STAGING_RESOURCE`
arch_man=`$DSM_GETPATH ARCHIVE_MANAGEMENT`
tools=`$DSM_GETPATH STAGING_TOOLS`
parent=$tools/parent_native_dsid.sh
firstf=$tools/first_file_for_latest_by_dsid.sh

Source_ID=`echo $cmip_dsid | cut -f4 -d.`
native_dsid=`$parent $cmip_dsid`

ts=`date -u +%Y%m%d_%H%M%S`
Report_Path="$opdir/RUN_REPORTS/run_record-$cmip_dsid-$ts"
mkdir -p $Report_Path
Report_Name="$Report_Path/Report-${cmip_dsid}-$ts"

echo "RUN REPORT for dsm_manage_CMIP_production test ====================================" >> $Report_Name
echo "" >> $Report_Name
echo "PRE-RUN Artifacts -----------------------------------" >> $Report_Name
arch_path=`cat $arch_man/Archive_Map | grep $native_dsid | cut -f3 -d,`
if [ -z $arch_path ]; then
    echo "    WARNING: Native DSID $native_dsid NOT FOUND in Archive Map $arch_map" >> $Report_Name
    echo ""
    # use warehouse dataset file if found
    first_file=`$firstf $native_dsid`
    # echo "DEBUG: firstfile = $first_file"
    case_id=`echo $first_file | cut -f1-3 -d.`
else
    case_id=`basename $arch_path`
fi

# experiment=`echo $case_id | cut -f3 -d.`
exp_w_vlab=`echo $cmip_dsid | cut -f5,6 -d.`
user_metadata=`ls $resource/CMIP6-Metadata/$Source_ID/ | grep $exp_w_vlab | tail -1`
full_metadata="$resource/CMIP6-Metadata/$Source_ID/$user_metadata"

echo "    Target CMIP dataset_id: $cmip_dsid" >> $Report_Name
echo "    Native E3SM dataset_id: $native_dsid" >> $Report_Name
echo "    Native Case ID:         $case_id" >> $Report_Name
echo "    Source Metadata file:   $full_metadata" >> $Report_Name
echo "" >> $Report_Name

IFS=$'\n'
echo "POST-RUN Artifacts -----------------------------------" >> $Report_Name
echo "" >> $Report_Name

echo "    ls -l $lockdir/$case_id/product" >> $Report_Name
out_text=`ls -l $lockdir/$case_id/product`
for aline in $out_text; do
    echo "        $aline" >> $Report_Name
done
echo "" >> $Report_Name

log_count=`ls $lockdir/$case_id/product/*.log | wc -l`
if [[ $log_count -gt 0 ]]; then
    typical_e2c_log=`ls $lockdir/$case_id/product/*.log | head -1`
    echo "    Content of e2c log $typical_e2c_log" >> $Report_Name
    echo "" >> $Report_Name

    cat $typical_e2c_log > junk_typic

    for aline in `cat junk_typic`; do
        echo "        $aline" >> $Report_Name
    done
    rm junk_typic
    echo "" >> $Report_Name
fi

facet_path=`echo $cmip_dsid | tr . /`
product_out="$lockdir/$case_id/product/$facet_path/$dsversion"
out_count=`ls $product_out | wc -l`
echo "Product files produced: $out_count"
echo "" >> $Report_Name
out_list=`ls -l $product_out`
for aline in $out_list; do
    echo "        $aline" >> $Report_Name
done
echo "" >> $Report_Name

mkdir -p $Report_Path/e2c_logs
for e2c_log in `ls $lockdir/$case_id/product/*.log`; do
    mv $e2c_log $Report_Path/e2c_logs
done

if [[ -d $lockdir/$case_id/product/cmor_logs ]]; then
    mv $lockdir/$case_id/product/cmor_logs $Report_Path
fi

dsm_gen_log=`ls $lockdir/dsmgen_logs | grep dsm_gen-$cmip_dsid | tail -1`
mv $lockdir/dsmgen_logs/$dsm_gen_log $Report_Path
echo "    EXAMINE dsm_generate_CMIP6 log: $dsm_gen_log" >> $Report_Name

dsm_sub_log="$lockdir/$case_id/caselogs/${cmip_dsid}.sublog"
mv $dsm_sub_log $Report_Path
echo "    EXAMINE dsm subordinate run log: ${cmip_dsid}.sublog" >> $Report_Name

dsm_sub="$lockdir/$case_id/scripts/${cmip_dsid}-gen_CMIP6.py"
mv $dsm_sub $Report_Path
echo "    EXAMINE subordinate run script: ${cmip_dsid}-gen_CMIP6.py"


otherlog=`ls $opdir | grep e3sm_to_cmip_run | tail -1`
mv $otherlog $Report_Path

rm -rf $opdir/e3sm_to_cmip_run*

rm -f EVIDENCE
