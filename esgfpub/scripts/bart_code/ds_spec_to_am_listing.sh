#!/bin/bash

ds_spec_list=$1
arch_map=/p/user_pub/e3sm/archive/.cfg/Archive_Map

output_list=`echo $ds_spec_list | cut -f2- -d_`

# echo $output_list

IFS=$'\n'

rm -f /tmp/this_temp_list

for ds_spec in `cat $ds_spec_list`; do
    # echo $ds_spec
    cat $arch_map | grep $ds_spec >> /tmp/this_temp_list

done

sort /tmp/this_temp_list | uniq > $output_list
