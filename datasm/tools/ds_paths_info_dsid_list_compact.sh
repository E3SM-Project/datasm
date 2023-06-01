#!/bin/bash

dsidlist=$1

dspi_compact=/p/user_pub/e3sm/staging/tools/ds_paths_info_compact.sh

for dsid in `cat $dsidlist`; do
    echo " "
    $dspi_compact $dsid
done
