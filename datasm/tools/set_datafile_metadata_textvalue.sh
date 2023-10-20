#!/bin/bash

# echo "TOOLS_DEBUG: argcount = $#"

if [ $# -ne 3 ]; then
    echo "Usage: $0 <datafile> <attribute_name> <attribute_text>"
    exit 1
fi

datafile=$1
att_name=$2
att_text=$3

ncatted --glb_att_add $att_name="$att_text" --hst $datafile
