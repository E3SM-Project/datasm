#!/bin/bash

awps_list=$1
arch_map=/p/user_pub/e3sm/archive/.cfg/Archive_Map

output_list=`echo $awps_list | cut -f2- -d_`

# echo $output_list

IFS=$'\n'

rm -f /tmp/this_temp_list

for aline in `cat $awps_list`; do
    ds_spec=`echo $aline | cut -f3 -d:`
    # echo $ds_spec
    cat $arch_map | grep $ds_spec >> /tmp/this_temp_list

done

sort /tmp/this_temp_list | uniq > $output_list
