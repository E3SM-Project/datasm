#!/bin/bash

source=/p/user_pub/e3sm/staging/prepub

model_versions=(1_1 1_1_ECA)

for version in ${model_versions[@]}
do
	for freq in `find $source/$version -type d -name "3hr" -or -name "3hr_snap" -or -name "day"`
        do
                echo `$freq | sed 's/\// /g' | awk '$12 != "model_output" {print $6, $7, $12}' | sed 's/ /./g' `
                #printf "\nStarting $freq \n"
                #python timerectifier.py $freq/ens1/v1 -j 75 --dryrun --gaps
        done
done
