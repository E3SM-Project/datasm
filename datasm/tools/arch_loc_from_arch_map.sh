#!/bin/bash

ts=`date -u +%Y%m%d_%H%M%S_%6N`

# must use Archive_Map
#       Campaign,Model,Experiment,Resolution,Ensemble,DatasetType,OutputType,ArchivePath,DatatypeTarExtractionPattern,Notes
# to create
#       Campaign,Model,Experiment,Resolution,Ensemble,ArchivePath

#cat /p/user_pub/e3sm/archive/.cfg/Archive_Map | cut -f1-6 -d, | sort | uniq | cut -f1-4,6 -d, | sort | uniq > arch_loc-$ts
cat /p/user_pub/e3sm/archive/.cfg/Archive_Map | cut -f1-5,8 -d, | sort | uniq > arch_loc-$ts
