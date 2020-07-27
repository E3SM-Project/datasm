#!/bin/bash

dirlist=$1

for aline in `cat $dirlist`; do
	chmod 755 $aline
	chmod 644 $aline/*
done
