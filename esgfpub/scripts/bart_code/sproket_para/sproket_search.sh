#!/bin/bash

sprok=/p/user_pub/e3sm/bartoletti1/abin/sproket-linux-0.2.13
config=/p/user_pub/e3sm/bartoletti1/Pub_Status/sproket/.sproket_config.json

# PYTHON: datetime.now().strftime('%Y%m%d_%H%M%S')

ts=`date +%Y%m%d.%H%M%S`

# to obtain ALL file names
$sprok -config $config -urls.only -no.download -no.verify -p 12 -y > E3SM_datafile_urls-$ts

exit 0

