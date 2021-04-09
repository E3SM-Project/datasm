#!/bin/bash

sprok=/p/user_pub/e3sm/bartoletti1/abin/sproket-linux-0.2.13
config=/p/user_pub/e3sm/bartoletti1/Pub_Status/sproket/.sproket_config.json

# PYTHON: datetime.now().strftime('%Y%m%d_%H%M%S')

ts=`date +%Y%m%d.%H%M%S`

# to obtain the datasetIDs
# $sprok -config $config -values.for dataset_id_template_ -no.download -no.verify -y > E3SM_dataset_ids


# $sprok -config $config -field.keys > E3SM_fields

IFS=$'\n'

for afield in `cat E3SM_Fields_useful`; do
	echo " " >> E3SM_field_values-$ts
	echo "FIELD_$afield" >> E3SM_field_values-$ts
	echo " " >> E3SM_field_values
	$sprok -config $config -values.for $afield >> E3SM_field_values-$ts
done


