#!/bin/bash

# given a derivative dataset_id (E3SM climo, timeseries, or CMIP6 publication)
# identify the parent native E3SM dataset_id from which the derivative was produced

# CMIP6 Example:        CMIP6.C4MIP.E3SM-Project.E3SM-1-1-ECA.hist-bgc.r1i1p1f1.3hr.pr.gr
#       project.activity.institution.sourceid.experiment.variant.freq.var.grid

if [ $# -eq 0 ]; then
    echo "NONE: No dataset_id supplied"
    exit 1
fi

dsid=$1


project=`echo $dsid | cut -f1 -d.`

if [ $project != "CMIP6" ]; then
    if [ $project == "E3SM" ]; then

        headpart=`echo $dsid | cut -f1-5 -d.`
        tailpart=`echo $dsid | cut -f8-9 -d.`
        src_dsid="${headpart}.native.model-output.${tailpart}"
        echo $src_dsid
        exit 0
        
    fi
    echo "NONE: unrecognized project: $project"
    exit 1

fi

# project is CMIP6

# First, split CMIP6 dataset_id to extract key values

comps=($(echo $dsid | tr . '\n'))

project=${comps[0]}
activity=${comps[1]}
institute=${comps[2]}
source_id=${comps[3]}
cmip_exp=${comps[4]}
variant=${comps[5]}
frequency=${comps[6]}
variable=${comps[7]}
grid=${comps[8]}

# we must set these 9 values correctly for the e2c command line

realm="NONE"
varname="NONE"
tables="NONE"
metadata="NONE"
mapfile="NONE"
outpath="NONE"
srcpath="NONE"
timeout="NONE"
logdir="NONE"

if [[ $institute != "E3SM-Project" && $institute != "UCSB" ]]; then
    echo "NONE: non-E3SM-Project not yet supported"
    exit 0
fi

modelversion=`echo $source_id | cut -f2- -d- | tr - _`  # CMIP6 dsids will NOT have "-LE" in the Source_ID
# what about HR campaign?

# HACK for NARRM
if [ $modelversion == "2_0" ]; then
    resolution="LR"
elif [ $modelversion == "2_0_NARRM" ]; then
    resolution="LR-NARRM"
else
    resolution="1deg_atm_60-30km_ocean"
fi

# HACK for v1_Large_Ensemble (External)
if [[ $modelversion == "1_0" && $institute == "UCSB" ]]; then
    modelversion="1_0_LE"
fi

# HACK for v2_Large_Ensemble (External)
if [[ $modelversion == "2_0" ]]; then
    rdex=`echo $variant | cut -f1 -di | cut -c2-`
    if [[ $rdex -ge 6 ]]; then
        modelversion="2_0_LE"
    fi
fi

grid="native"
out_type="model-output"
ensn=`echo $variant | cut -f1 -di | cut -c2-`
ens="ens$ensn"


# setup realm

if [ $frequency == "SImon" ]; then
    realm="sea-ice"
else
    hit1=`echo "3hr AERmon Amon CFmon day fx" | grep -w $frequency | wc -l`
    hit2=`echo "LImon Lmon" | grep -w $frequency | wc -l`
    hit3=`echo "Ofx Omon" | grep -w $frequency | wc -l`
    if [ $hit1 -eq 1 ]; then
        realm="atmos"
    elif [ $hit2 -eq 1 ]; then
        realm="land"
    elif [ $hit3 -eq 1 ]; then
        realm="ocean"
    fi
fi

if [ "$realm" == "NONE" ]; then
    echo "NONE: unrecognized realm from CMIP freq: $frequency"
    exit 1
fi

# setup freq

hit_mon=`echo AERmon Amon CFmon LImon Lmon Omon SImon | grep -w $frequency | wc -l`
if [ $hit_mon -eq 1 ]; then
    freq="mon"
elif [ $frequency == "fx" ] || [ $frequency == "Ofx" ]; then
    freq="mon"  # output = fixed
else
    freq=$frequency
fi

# setup experiment

if [ ${modelversion:0:3} == "1_1" ]; then
    if [ $cmip_exp == "hist-bgc" ]; then
        experiment="hist-BDRC"
    elif [ $cmip_exp == "historical" ]; then
        experiment="hist-BDRD"
    elif [ $cmip_exp == "ssp585-bgc" ]; then
        experiment="ssp585-BDRC"
    elif [ $cmip_exp == "ssp585" ]; then
        experiment="ssp585-BDRD"
    fi
else
    experiment=$cmip_exp
fi

# we must forge  E3SM.modelversion.resolution.realm.grid.output_type.freq.ensemble
# E3SM Example:         E3SM.1_0.hist-GHG.1deg_atm_60-30km_ocean.land.native.model-output.mon.ens1


echo "E3SM.${modelversion}.${experiment}.${resolution}.${realm}.${grid}.${out_type}.${freq}.${ens}"

