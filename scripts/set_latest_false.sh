#!/bin/bash

# the version should use a 'v' not a '#'


wget --no-check-certificate --ca-certificate ~/.globus/certificate-file \
         --certificate ~/.globus/certificate-file \
         --private-key ~/.globus/certificate-file --verbose \
         "https://esgf-node.llnl.gov/esg-search/ws/updateById?id=${1}|aims3.llnl.gov&action=set&field=latest&value=false&core=datasets"