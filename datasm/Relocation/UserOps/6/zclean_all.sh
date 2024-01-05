#!/bin/bash

for odir in Ops1 Ops2 Ops3 Ops4 Ops5 Ops6; do
    cd $odir
    ../zclean.sh
    cd ..
done
