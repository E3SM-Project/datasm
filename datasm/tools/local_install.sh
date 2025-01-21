#!/bin/bash

# must execute in tools directory

resource=`$DSM_GETPATH STAGING_RESOURCE`
tools=`$DSM_GETPATH STAGING_TOOLS`

cp ../resource/dataset_spec.yaml $resource/dataset_spec.yaml
cp ../resource/derivatives.conf $resource/derivatives.conf
cp ../resource/Archive_Locator $resource/archive/Archive_Locator
cp ../resource/Archive_Map $resource/archive/Archive_Map
cp ../resource/Archive_Map_headers $resource/archive/Archive_Map_headers
cp ../resource/Standard_Datatype_Extraction_Patterns $resource/archive/Standard_Datatype_Extraction_Patterns

for afile in `cat MANIFEST`; do
    cp $afile $tools
done


