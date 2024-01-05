#!/bin/bash

dsidlist=$1

if [ $# -eq 0 ]; then
    echo "Usage: 2_get_al_sdep.sh <dsid_list> [arch_locator_file]"
    exit 0
fi

# for each dsid Proj.Model.Exper.Resol.Realm.Gridv.Otype.Freqv.Ensem
# create key = Model,Exper,Resol,Ensem
# use key to find Archive_Loc line
#       Campaign,Model,Exper,Resol,Ensem,Archive_Path
# if NOT found, echo "$dsid:NO_ARCH_LOC_DATA"
# else
#    build up AL_list with "$dsid:$al_line(s)" (one line per)
#    obtain Campaign from Arch_Loc_line
#    build up SDEP_list from Campaign, Otype, realm, grid, freq

archman=`$DSM_GETPATH ARCHIVE_MANAGEMENT`
arch_loc=$archman/Archive_Locator
sdepfile=$archman/Standard_Datatype_Extraction_Patterns

if [ $# -eq 2 ]; then
    arch_loc=$2
fi

rm -f zFinal

for dsid in `cat $dsidlist`; do
    mere=`echo $dsid | cut -f2,3,4,9 -d. | tr . ,`
    al_lines=`cat $arch_loc | grep ${mere},`
    if [ -z $al_lines ]; then
        echo $dsid:NO_ARCH_LOC_DATA
        continue
    fi
    campaign="NO_CAMP"
    for aline in $al_lines; do
        campaign=`echo $aline | cut -f1 -d,`
        # echo $dsid:AL:$aline
    done
    realm=`echo $dsid | cut -f5 -d.`
    gridv=`echo $dsid | cut -f6 -d.`
    freqv=`echo $dsid | cut -f8 -d.`
    s_key="${realm}.${gridv}.${freqv},"

    s_can=`cat $sdepfile | grep $s_key | grep $campaign`

    echo "$s_can" >> zFinal
done

cat zFinal | sort | uniq
rm -f zFinal
    
