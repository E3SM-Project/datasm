#!/bin/bash

ts=`date -u +%Y%m%d_%H%M%S_%6N`

ampath=`$DSM_GETPATH ARCHIVE_MANAGEMENT`

# must use Archive_Map
#       Campaign,Model,Experiment,Resolution,Ensemble,DatasetType,OutputType,ArchivePath,DatatypeTarExtractionPattern,Notes
# to create
#       Campaign,Model,Experiment,Resolution,Ensemble,ArchivePath

#cat [STAGING_RESOURCE]/archive/Archive_Map | cut -f1-6 -d, | sort | uniq | cut -f1-4,6 -d, | sort | uniq > arch_loc-$ts
# cat $ampath/Archive_Map | cut -f1-5,8 -d, | sort | uniq > arch_loc-$ts

tmpf="tmpf-$ts"

for aline in `cat $ampath/Archive_Map`; do
    camp=`echo $aline | cut -f1 -d,`
    dsid=`echo $aline | cut -f2 -d,`
    aloc=`echo $aline | cut -f3 -d,`
    keyv=`echo $dsid | cut -f2-4,9 -d. | tr . ,`
    echo $camp,$keyv,$aloc >> $tmpf
done

cat $tmpf | sort | uniq > arch_loc-$ts

rm $tmpf


    
