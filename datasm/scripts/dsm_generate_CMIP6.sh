#!/bin/bash

# Conda Environment
# -----------------
# Either comment out the lines below to use the E3SM Unified environment
# or use a local environment.
#source /usr/local/e3sm_unified/envs/load_latest_e3sm_unified_acme1.sh
#conda activate e2c_nco511_13

if [[ $# -lt 2 || $1 == "--help" ]]; then
    echo "Usage: $0 <mode> <file_of_cmip6_dataset_ids>"
    echo "       <mode> must be either \"TEST\" or \"WORK\"."
    echo "       In TEST mode, only the first year of data will be processed,"
    echo "       and the E3SM dataset status files are not updated."
    echo "       In WORK mode, all years given in the dataset_spec are applied,"
    echo "       and the E3SM dataset status files are updated, and the cmorized"
    echo "       results are moved to staging data (the warehouse)."
    exit 0
fi

dryrun=0
dryrun2=0

# Charlie debug test
# ncclimo=`realpath ~zender1/bin/ncclimo`
ncclimo=ncclimo


# -----------------------------------------------------------
# Resources and Tools
# -----------------------------------------------------------

resource=`$DSM_GETPATH STAGING_RESOURCE`
wh_root=`$DSM_GETPATH STAGING_DATA`
cmor_tables=$resource/cmor/cmip6-cmor-tables/Tables
tools=`$DSM_GETPATH STAGING_TOOLS`
parent_dsid=$tools/parent_native_dsid.sh
latest_data=$tools/latest_data_location_by_dsid.sh
cmip6params=$tools/cmip6_parameters.sh
get_case_id=$tools/case_id_for_e3sm_dsid.sh
get_years=$tools/tell_years_dsid.py
metadata_version=$tools/metadata_version.py

vrt_remap_plev19=$resource/grids/vrt_remap_plev19.nc

# -----------------------------------------------------------
# Convenient Function Definitions
# -----------------------------------------------------------

table_realm () {
    for tab in 3hr AERmon Amon CFmon day fx; do
        if [[ $tab == $1 ]]; then
            echo "atm"
            return
        fi
    done
    if [[ $1 == "Lmon" || $1 == "LImon" ]]; then
        echo "lnd"
        return
    elif [[ $1 == "Ofx" || $1 == "Omon" ]]; then
        echo "mpaso"
        return
    elif [[ $1 == "SImon" ]]; then
        echo "mpassi"
        return
    else
        echo "UNKN_REALM_FOR_$1"
        return
    fi
}

# 'mon', 'day', '6hrLev', '6hrPlev', '6hrPlevPt', '3hr', '1hr.
table_freq () {
    if [[ $1 == "3hr" ]]; then
        echo "3hr"
    elif [[ $1 == "day" ]]; then
        echo "day"
    elif [[ $1 == "6hrLev" ]]; then
        echo "6hrLev"
    elif [[ $1 == "6hrPlev" ]]; then
        echo "6hrPlev"
    elif [[ $1 == "6hrPlevPt" ]]; then
        echo "6hrPlevPt"
    elif [[ $1 == "1hr" ]]; then
        echo "1hr"
    else
        echo "mon"
    fi

    return
}

in_list() {

    invar=$1
    shift
    for tval in $@; do
        if [[ $invar == $tval ]]; then
            echo 1
            return
        fi
    done
    echo 0
}

CVatm3d=( hus o3 ta ua va zg hur wap )
CVatmfx=( areacella orog sftlf )
CVatmdy=( tasmin tasmax tas huss rlut pr )
CVatm3h=( pr )
CVlnd=( mrsos mrso mrfso mrros mrro prveg evspsblveg evspsblsoi tran tsl lai )
CVmpaso=( areacello fsitherm hfds masso mlotst sfdsi sob soga sos sosga tauuo tauvo thetaoga tob tos tosga volo wfo zos thetaoga hfsifrazil masscello so thetao thkcello uo vo volcello wo zhalfo )
CVmpassi=( siconc sitemptop sisnmass sitimefrac siu siv sithick sisnthick simass )

var_type () {
    var=$1
    realm=$2
    freq=$3

    if [[ $realm == "atm" ]]; then
        if [[ $freq == "mon" ]]; then
            if [[ `in_list $var ${CVatm3d[@]}` -eq 1 ]]; then
                echo "atm_mon_3d"
                return
            elif [[ `in_list $var ${CVatmfx[@]}` -eq 1 ]]; then
                echo "atm_mon_fx"
                return
            else
                echo "atm_mon_2d"       # default for mon
                return
            fi
        elif [[ $freq == "day" && `in_list $var ${CVatmdy[@]}` -eq 1 ]]; then
            echo "atm_day"
            return
        elif [[ $freq == "3hr" && `in_list $var ${CVatm3h[@]}` -eq 1 ]]; then
            echo "atm_3hr"
            return
        else
            echo "NONE"
            return
        fi
    elif [[ $realm == "lnd" ]]; then
        if [[ $freq == "mon" ]]; then
            if [[ `in_list $var ${CVlnd[@]}` -eq 1 ]]; then
                echo "lnd_mon"
                return
            elif [[ $var == "snw" ]]; then
                echo "lnd_ice_mon"
                return
            else
                echo "NONE"
                return
            fi
        else
            echo "NONE"
            return
        fi
    elif [[ $realm == "mpaso" ]]; then
        if [[ $freq == "mon" ]]; then
            if [[ `in_list $var ${CVmpaso[@]}` -eq 1 ]]; then
                echo "mpaso_mon"
                return
            else
                echo "NONE"
                return
            fi
        else
            echo "NONE"
            return
        fi
    elif [[ $realm == "mpassi" ]]; then
        if [[ $freq == "mon" ]]; then
            if [[ `in_list $var ${CVmpassi[@]}` -eq 1 ]]; then
                echo "mpassi_mon"
                return
            else
                echo "NONE"
                return
            fi
        else
            echo "NONE"
            return
        fi
    else
        echo "NONE"
    fi
}

suggest_ypf () {
    if [[ $1 == "lnd_mon" || $1 == "atm_mon_2d" || $1 == "atm_mon_3d" || $1 == "atm_mon_fx" ]]; then
        echo 50
    else
        echo 10
    fi
}

the_pwd=`pwd`
thisdir=`realpath $the_pwd`
mkdir -p $thisdir/tmp
mkdir -p $thisdir/tmp/info_yaml

app_name=$0
run_mode=$1
dsidlist=$2

#### =====================================================================
#### BEGIN PROCESSING CMIP6 DATASET_IDS                               ####
#### =====================================================================

for dsid in `cat $dsidlist`; do
    cmip_src_id=`echo $dsid | cut -f4 -d.`
    cmip_experi=`echo $dsid | cut -f5 -d.`
    cmip_vlabel=`echo $dsid | cut -f6 -d.`
    table_name=`echo $dsid | cut -f7 -d.`
    cmip6var=`echo $dsid | cut -f8 -d.`
    exp=$cmip_experi
    freq=`table_freq $table_name`
    realm=`table_realm $table_name`
    nat_dsid=`$parent_dsid $dsid`
    native_src=`$latest_data $nat_dsid`
    the_var_type=`var_type $cmip6var $realm $freq`

    caseid=`$get_case_id $nat_dsid`

    if [[ $dryrun2 -eq 1 ]]; then
        echo "caseid: $caseid, the_var_type: $the_var_type"
        continue
    fi

    # initialize run_mode variations
    years=`python $get_years -d $nat_dsid`
    start_yr=`echo $years | cut -f1 -d,`
    if [[ $run_mode == "TEST" ]]; then
        end_yr=$start_yr
        ypf=1
    else
        end_yr=`echo $years | cut -f2 -d,`
        status_file=`$tools/ensure_status_file_for_dsid.sh $dsid`
        # need intelligence here    
        ypf=`suggest_ypf $the_var_type`
    fi

    # tmp/info_yaml and tmp/tmp_params need not be persisted across cases

    yamlfile=$thisdir/tmp/info_yaml/${table_name}_${cmip6var}.yaml
    e3sm_to_cmip --info -v $cmip6var --freq $freq --realm $realm -t $cmor_tables --map no_map --info-out $yamlfile 2> err_log
    nat_vars=`cat $yamlfile | grep "E3SM Variables" | cut -f2 -d: | tr -d ' '`

    # Obtain namefile, restartfile, map_file and regionfile, as appropriate
    $cmip6params $dsid > $thisdir/tmp/tmp_params
    namefile=`cat $thisdir/tmp/tmp_params | grep namefile | cut -f2 -d:`
    restartf=`cat $thisdir/tmp/tmp_params | grep restart | cut -f2 -d:`
    map_file=`cat $thisdir/tmp/tmp_params | grep mapfile | cut -f2 -d:`
    region_f=`cat $thisdir/tmp/tmp_params | grep region | cut -f2 -d:`

    # begin persistent storage across cases.

    # setup workdir names =====
    casedir=$thisdir/tmp/$caseid
    subscripts=$casedir/scripts
    log_dir=$casedir/caselogs
    native_data=$casedir/native_data
    native_out=$casedir/native_out
    rgr_dir=$casedir/rgr
    rgr_dir_fixed=$casedir/rgr_fixed_vars
    rgr_dir_vert=$casedir/rgr_vert
    result_dir=$casedir/product
    mkdir -p $log_dir $native_data $native_out $rgr_dir $rgr_dir_fixed $rgr_dir_vert $result_dir $subscripts

    rm -f $native_data/* $native_out/* $rgr_dir/* $rgr_dir_fixed/* $rgr_dir_vert/*
    if [[ $run_mode == "WORK" ]]; then
        rm -rf $result_dir/*
        mkdir -p $result_dir
    fi

    # Copy correct metadata file and edit for version = TODAY
    metadata_dst=$casedir/metadata
    mkdir -p $metadata_dst
    metadata_name=${exp}_${cmip_vlabel}.json
    metadata_src=$resource/CMIP6-Metadata/$cmip_src_id/$metadata_name
    cp $metadata_src $metadata_dst
    metadata_file=$metadata_dst/$metadata_name
    python $metadata_version -i $metadata_file -m set

    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    logfile="$log_dir/$ts-${dsid}.log"

    echo "       ISSUED: $0 $1 $2 (dryrun=$dryrun)" >> $logfile
    echo "     run_mode: $run_mode" >> $logfile
    echo "   CMIP6_dsid: $dsid" >> $logfile
    echo "  cmip_src_id: $cmip_src_id" >> $logfile
    echo "  cmip_vlabel: $cmip_vlabel" >> $logfile
    echo "          exp: $exp" >> $logfile
    echo "       caseid: $caseid" >> $logfile
    echo "        start: $start_yr" >> $logfile
    echo "          end: $end_yr" >> $logfile
    echo "          ypf: $ypf" >> $logfile
    echo "        realm: $realm" >> $logfile
    echo "         freq: $freq" >> $logfile
    echo "     var_type: $the_var_type" >> $logfile
    echo "     cmip6var: $cmip6var" >> $logfile
    echo "     nat_vars: $nat_vars" >> $logfile
    echo "   native_src: $native_src" >> $logfile
    echo "     namefile: $namefile" >> $logfile
    echo "     restartf: $restartf" >> $logfile
    echo "     map_file: $map_file" >> $logfile
    echo "     region_f: $region_f" >> $logfile
    echo "     metadata: $metadata_file" >> $logfile
    echo "  cmor_tables: $cmor_tables" >> $logfile
    echo "" >> $logfile


    # produce symlinks to native source in native_data

    if [[ $the_var_type == "atm_mon_fx" ]]; then
        # special case, just 1 file whether TEST or WORK mode
        for afile in `ls $native_src | head -1`; do
            ln -s $native_src/$afile $native_data/$afile 2>/dev/null
        done
    elif [[ $run_mode == "TEST" ]]; then
        if [[ $the_var_type == "mpaso_mon" || $the_var_type == "mpassi_mon" ]]; then
            # obtain the "year code" for first year.  May be "0001", or "1850", etc
            year_tag=`ls $native_src | head -1 | rev | cut -f2 -d. | rev | cut -f1 -d-`
            for afile in `ls $native_src/*.${year_tag}-*.nc`; do
                afile=`basename $afile`
                ln -s $native_src/$afile $native_data/$afile 2>/dev/null
            done
        else
            # NOTE: For non-MPAS, the regridding step accepts start and end year, which
            # automatically limits the input files presented to e3sm_to_cmip
            for afile in `ls $native_src`; do
                ln -s $native_src/$afile $native_data/$afile 2>/dev/null
            done
        fi
    else  # WORK mode
        for afile in `ls $native_src`; do
            ln -s $native_src/$afile $native_data/$afile 2>/dev/null
        done
    fi

    # For MPAS, also place symlinks to restart, namefile and regionfile into the
    # native_data directory.

    if [[ $realm == "mpaso" || $realm == "mpassi" ]]; then
        if [[ $restartf != "NONE" ]]; then
            restart_base=`basename $restartf`
            ln -s $restartf $native_data/$restart_base 2>/dev/null
        fi 
        if [[ $namefile != "NONE" ]]; then
            namefile_base=`basename $namefile`
            ln -s $namefile $native_data/$namefile_base 2>/dev/null
        fi 
        if [[ $region_f != "NONE" ]]; then
            region_f_base=`basename $region_f`
            ln -s $region_f $native_data/$region_f_base 2>/dev/null
        fi 
    fi

    #
    # Create the var-type specific command lines ============================================
    #

    # common flags
    flags='-7 --dfl_lvl=1 --no_cll_msr --no_stdin'

    if [[ $the_var_type == "atm_mon_2d" ]]; then
        cmd_1="$ncclimo -P eam -j 1 --map=${map_file} --start=$start_yr --end=$end_yr --ypf=$ypf --split -c $caseid -o ${native_out} -O ${rgr_dir} -v ${nat_vars} -i ${native_data} ${flags}"
        cmd_2="e3sm_to_cmip -v $cmip6var -u $metadata_file -t $cmor_tables -o $result_dir -i ${rgr_dir}"
    elif [[ $the_var_type == "atm_mon_fx" ]]; then
        cmd_1="ncremap --map=${map_file} -v $nat_vars -I ${native_data} -O ${rgr_dir_fixed} --no_stdin"
        cmd_2="e3sm_to_cmip -v $cmip6var -u $metadata_file -t $cmor_tables -o $result_dir -i ${rgr_dir_fixed} --realm fx"
    elif [[ $the_var_type == "atm_mon_3d" ]]; then
        cmd_1="$ncclimo -P eam -j 1 --map=${map_file} --start=$start_yr --end=$end_yr --ypf=$ypf --split -c $caseid -o ${native_out} -O ${rgr_dir_vert} -v ${nat_vars} -i ${native_data} ${flags}"
        cmd_1b="ncks --rgr xtr_mth=mss_val --vrt_fl=$vrt_remap_plev19 ${rgr_dir_vert}/\$afile ${rgr_dir}/\$afile"
        cmd_2="e3sm_to_cmip -v $cmip6var -u $metadata_file -t $cmor_tables -o $result_dir -i ${rgr_dir}"
    elif [[ $the_var_type == "atm_day" ]]; then
        flags="$flags --clm_md=hfs"
        # flags="$flags --tpd=1"
        cmd_1="$ncclimo -P eam -j 1 --map=${map_file} --start=$start_yr --end=$end_yr --ypf=$ypf --split -c $caseid -o ${native_out} -O ${rgr_dir} -v ${nat_vars} -i ${native_data} ${flags}"
        cmd_2="e3sm_to_cmip -v $cmip6var -u $metadata_file -t $cmor_tables -o $result_dir -i ${rgr_dir} --freq day"
    elif [[ $the_var_type == "atm_3hr" ]]; then
        flags="$flags --clm_md=hfs"
        cmd_1="$ncclimo -P eam -j 1 --map=${map_file} --start=$start_yr --end=$end_yr --ypf=$ypf --split -c $caseid -o ${native_out} -O ${rgr_dir} -v ${nat_vars} -i ${native_data} ${flags}"
        cmd_2="e3sm_to_cmip -v $cmip6var -u $metadata_file -t $cmor_tables -o $result_dir -i ${rgr_dir} --freq 3hr"
    elif [[ $the_var_type == "lnd_mon" || $the_var_type == "lnd_ice_mon" ]]; then
        cmd_1="$ncclimo -P elm -j 1 --var_xtr=landfrac --map=${map_file} --start=$start_yr --end=$end_yr --ypf=$ypf --split -c $caseid -o ${native_out} -O ${rgr_dir} -v ${nat_vars} -i ${native_data} ${flags}"
        cmd_2="e3sm_to_cmip -v $cmip6var -u $metadata_file -t $cmor_tables -o $result_dir -i ${rgr_dir}"
    elif [[ $the_var_type == "mpaso_mon" ]]; then
        cmd_2="e3sm_to_cmip -v $cmip6var -u $metadata_file -t $cmor_tables -o $result_dir -i $native_data -s --realm Omon --map ${map_file}"
    elif [[ $the_var_type == "mpassi_mon" ]]; then
        cmd_2="e3sm_to_cmip -v $cmip6var -u $metadata_file -t $cmor_tables -o $result_dir -i $native_data -s --realm SImon --map ${map_file} --logdir ZLogs"
    else
        echo "ERROR: var_type() returned $the_var_type for cmip dataset_id $dsid" >> $logfile 2>&1
        continue
    fi

    echo "CMD 1:   $cmd_1" >> $logfile 2>&1
    echo "CMD 1b:  $cmd_1b" >> $logfile 2>&1
    echo "CMD 2:   $cmd_2" >> $logfile 2>&1

    # confirm presence of data (symlinks) in native_data
    in_count=`ls $native_data | wc -l`
    echo "NATIVE_SOURCE_COUNT=$in_count files ($((in_count / 12)) years)" >> $logfile

    #
    # Generate an isolated execution script that can be called from this script, or manually
    #
    # IMPORTANT: for large MPAS jobs, this script must be designed to establish the input symlinks by decade,
    # and issue the "cmd2" (e3sm_ to_cmip call) for each separate decade.
    #
    escript="$subscripts/$dsid-Generate_CMIP6.sh"

    rm -f $escript

    echo "#!/bin/bash" >> $escript
    echo "" >> $escript
    echo "" >> $escript
    echo "ts=\`date -u +%Y%m%d_%H%M%S_%6N\`" >> $escript
    echo "the_sublog=$log_dir/\$ts-${dsid}.sublog" >> $escript
    echo "" >> $escript

    echo "# confirm presence of data (symlinks) in native_data" >> $escript
    echo "in_count=\`ls $native_data | wc -l\`" >> $escript
    echo "echo \"NATIVE_SOURCE_COUNT=\$in_count files (\$((in_count / 12)) years)\" >> \$the_sublog 2>&1" >> $escript
    echo "" >> $escript

    # ==== Call NCO stuff first if not MPAS ====================================

    if [[ $the_var_type != "mpaso_mon" && $the_var_type != "mpassi_mon" ]]; then
        echo "${cmd_1} >> \$the_sublog 2>&1" >> $escript
        echo "" >> $escript
        echo "ret_code=\$?" >> $escript
        echo "    ts=\`date -u +%Y%m%d_%H%M%S_%6N\`" >> $escript
        echo "if [[ \$ret_code -ne 0 ]]; then" >> $escript
        echo "    echo \"\$ts:ERROR:  NCO Process Fail: exit code = \$ret_code\" >> \$the_sublog 2>&1" >> $escript
        if [[ $run_mode == "WORK" ]]; then
            echo "    echo \"COMM:\$ts:POSTPROCESS:DSM_Generate_CMIP6:NCO:Fail:return_code=\$ret_code\" >> $status_file" >> $escript
            echo "    echo \"STAT:\$ts:POSTPROCESS:DSM_Generate_CMIP6:Fail:return_code=\$ret_code\" >> $status_file" >> $escript
        fi
        echo "    exit \$ret_code" >> $escript
        echo "fi" >> $escript
        echo "" >> $escript
        echo "echo \"\$ts:NCO Process Pass: Regridding Successful\" >> \$the_sublog 2>&1" >> $escript
        if [[ $run_mode == "WORK" ]]; then
            echo "echo \"COMM:\$ts:POSTPROCESS:DSM_Generate_CMIP6:NCO:Pass\" >> $status_file" >> $escript
        fi

        if [[ $the_var_type == "atm_mon_3d" ]]; then
            echo "rgr_dir_vert=$rgr_dir_vert" >> $escript
            echo "for afile in \`ls \${rgr_dir_vert}\`; do" >> $escript
            echo "    ${cmd_1b} >> \$the_sublog 2>&1" >> $escript
            echo "done" >> $escript
            echo "" >> $escript
        fi
    fi

    # ==== Call E2C stuff now, if we are still here ====================================
    # if mpaso, must loop on decades

    if [[ $the_var_type == "mpaso_mon" && $run_mode == "WORK" ]]; then
        # echo "engineer codes to loop on ypf=20 years here"

        echo "native_src=$native_src" >> $escript
        echo "start_year=\`ls \$native_src | rev | cut -f2 -d. | rev | cut -f1 -d- | head -1\`" >> $escript
        echo "final_year=\`ls \$native_src | rev | cut -f2 -d. | rev | cut -f1 -d- | tail -1\`" >> $escript
        echo "range_years=\$((10#\$final_year - 10#\$start_year + 1))" >> $escript
        echo "ypf=20" >> $escript
        echo "range_segs=\$((range_years/ypf))" >> $escript
        echo "if [[ \$((range_segs*ypf)) -lt \$range_years ]]; then range_segs=\$((range_segs + 1)); fi" >> $escript
        echo "" >> $escript
        echo "native_data=$native_data" >> $escript
        echo "" >> $escript
        echo "ts2=\`date -u +%Y%m%d_%H%M%S_%6N\`" >> $escript
        echo "echo \"\$ts2:Begin processing \$range_years years at \$ypf years per segment. Total segments = \$range_segs\" >> \$the_sublog 2>&1" >> $escript
        echo "" >> $escript
        echo "for ((segdex=0;segdex<range_segs;segdex++)); do" >> $escript
        echo "" >> $escript
        echo "    # wipe existing native_data datafile symlinks, create new range of same" >> $escript
        echo "    # then create the next segment of symlinks, and call the e3sm_to_cmip" >> $escript
        echo "" >> $escript
        echo "    for afile in \`ls \${native_data}/*mpaso.hist.am.timeSeriesStatsMonthly*.nc\`; do" >> $escript
        echo "        rm -f \$afile" >> $escript
        echo "    done" >> $escript
        echo "" >> $escript
        echo "    for ((yrdex=0;yrdex<ypf;yrdex++)); do" >> $escript
        echo "        the_year=\$((10#\$start_year + segdex*ypf + yrdex))" >> $escript
        echo "        prt_year=\`printf \"%04d\" \"\$the_year\"\`" >> $escript
        echo "" >> $escript
        echo "        for afile in \`ls \$native_src/*.\${prt_year}-*.nc\`; do" >> $escript
        echo "            bfile=\`basename \$afile\`" >> $escript
        # echo "            echo \"DEBUG: ADDING NEW LINK: \${native_data}/\$bfile\" >> \$the_sublog 2>&1" >> $escript
        echo "            ln -s \$afile \$native_data/\$bfile 2>/dev/null" >> $escript
        echo "        done" >> $escript
        echo "    done" >> $escript
        echo "" >> $escript
        echo "    ${cmd_2} >> \$the_sublog 2>&1" >> $escript
        echo "" >> $escript
        echo "    ret_code=\$?" >> $escript
        echo "    ts2=\`date -u +%Y%m%d_%H%M%S_%6N\`" >> $escript
        echo "    if [[ \$ret_code -ne 0 ]]; then" >> $escript
        echo "        echo \"\$ts2:ERROR:  E2C Process Fail: exit code = \$ret_code\" >> \$the_sublog 2>&1" >> $escript
        if [[ $run_mode == "WORK" ]]; then
            echo "        echo \"COMM:\$ts2:POSTPROCESS:DSM_Generate_CMIP6:E2C:Fail:return_code=\$ret_code\" >> $status_file" >> $escript
            echo "        echo \"STAT:\$ts2:POSTPROCESS:DSM_Generate_CMIP6:Fail:return_code=\$ret_code\" >> $status_file" >> $escript
        fi
        echo "        exit \$ret_code" >> $escript
        echo "    fi" >> $escript
        echo "    echo \"\$ts2:Completed years to \$prt_year\" >> \$the_sublog 2>&1" >> $escript
        echo "done" >> $escript
        echo "" >> $escript

    else        # non-looping one-pass form
    
        echo "${cmd_2} >> \$the_sublog 2>&1" >> $escript
        echo "" >> $escript
        echo "ret_code=\$?" >> $escript
        echo "ts2=\`date -u +%Y%m%d_%H%M%S_%6N\`" >> $escript
        echo "if [[ \$ret_code -ne 0 ]]; then" >> $escript
        echo "    echo \"\$ts:ERROR:  E2C Process Fail: exit code = \$ret_code\" >> \$the_sublog 2>&1" >> $escript
        if [[ $run_mode == "WORK" ]]; then
            echo "    echo \"COMM:\$ts2:POSTPROCESS:DSM_Generate_CMIP6:E2C:Fail:return_code=\$ret_code\" >> $status_file" >> $escript
            echo "    echo \"STAT:\$ts2:POSTPROCESS:DSM_Generate_CMIP6:Fail:return_code=\$ret_code\" >> $status_file" >> $escript
        fi
        echo "    exit \$ret_code" >> $escript
        echo "fi" >> $escript
    fi

    echo "echo \"\$ts2:E2C Process Pass: Cmorizing Successful\" >> \$the_sublog 2>&1" >> $escript 
    if [[ $run_mode == "WORK" ]]; then
        echo "echo \"COMM:\$ts2:POSTPROCESS:DSM_Generate_CMIP6:E2C:Pass\" >> $status_file" >> $escript
        echo "echo \"COMM:\$ts2:POSTPROCESS:DSM_Generate_CMIP6:Pass\" >> $status_file" >> $escript
    fi
    echo "" >> $escript
    echo "exit 0" >> $escript

    chmod 750 $escript

    echo "Produced for eval:  escript=$escript" >> $logfile

    if [[ $dryrun -eq 1 ]]; then
        continue
    fi

    # 
    # SECTION:  Subscript Execution ==========================================================
    #

    ts1=`date -u +%Y%m%d_%H%M%S_%6N`
    echo "$ts1: Begin Processing dataset_id: $dsid (the_var_type = $the_var_type)" >> $logfile

    if [[ $run_mode == "WORK" ]]; then
        echo "STAT:$ts1:POSTPROCESS:DSM_Generate_CMIP6:Engaged" >> $status_file
    fi

    ${escript} >> $logfile 2>&1
    ret_code=$?

    if [[ $ret_code -ne 0 ]]; then
        echo "ERROR:  Subprocess exit code = $ret_code" >> $logfile 2>&1
        if [[ $run_mode == "WORK" ]]; then
            ts=`date -u +%Y%m%d_%H%M%S_%6N`
            echo "COMM:$ts:POSTPROCESS:DSM_Generate_CMIP6:Subprocess:Fail:return_code=$ret_code" >> $status_file
            echo "STAT:$ts:POSTPROCESS:GenerateCMIP:Fail:return_code=$ret_code" >> $status_file
        fi
        continue
    fi
    
    #
    # SECTION:  Product Disposition  =========================================================
    #

    ts2=`date -u +%Y%m%d_%H%M%S_%6N`

    # if run_mode == WORK, Forge the warehouse destination facet-path and move the product
    product_dst=""
    if [[ $run_mode == "WORK" ]]; then
        facet_path=`echo $dsid | tr . /`
        ds_version=`cat $metadata_file | fgrep \"version\": | cut -f2 -d: | tr -d \" | tr -d ' '`
        product_src=$result_dir/$facet_path/$ds_version
        product_dst=${wh_root}/$facet_path/$ds_version
        mkdir -p $product_dst
        # move the tmp/<caseid>/products files to the product_dst location
        mv $product_src/*.nc $product_dst
        echo "Completed move of CMIP6 dataset to Staging Data ($product_dst)." >> $logfile
    fi

    ts3=`date -u +%Y%m%d_%H%M%S_%6N`
    echo "$ts3: Completed Processing dataset_id: $dsid" >> $logfile
    if [[ $run_mode == "WORK" ]]; then
        echo "COMM:$ts3:POSTPROCESS:DSM_Generate_CMIP6:Pass" >> $status_file
        # echo "COMM:$ts3:POSTPROCESS:GenerateCMIP:Pass" >> $status_file
        # echo "STAT:$ts3:POSTPROCESS:Pass" >> $status_file
    fi

done

exit 0
    
