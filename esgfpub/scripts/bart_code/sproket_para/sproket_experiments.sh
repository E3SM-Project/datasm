#!/bin/bash

sprok=/p/user_pub/e3sm/bartoletti1/abin/sproket-linux-0.2.13
config=/p/user_pub/e3sm/bartoletti1/Pub_Status/sproket/.sproket_config.json

# PYTHON: datetime.now().strftime('%Y%m%d_%H%M%S')

IFS=$'\n'

$sprok -config $config -values.for experiment


