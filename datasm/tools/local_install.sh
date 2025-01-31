#!/bin/bash

# must execute in tools directory

resource=`$DSM_GETPATH STAGING_RESOURCE`
tools=`$DSM_GETPATH STAGING_TOOLS`

cp ../resources/dataset_spec.yaml $resource/dataset_spec.yaml
cp ../resources/derivatives.conf $resource/derivatives.conf
cp ../resources/Archive_Locator $resource/archive/Archive_Locator
cp ../resources/Archive_Map $resource/archive/Archive_Map
cp ../resources/Archive_Map_headers $resource/archive/Archive_Map_headers
cp ../resources/NERSC_Archive_Map $resource/archive/NERSC_Archive_Map
cp ../resources/Standard_Datatype_Extraction_Patterns $resource/archive/Standard_Datatype_Extraction_Patterns

for afile in `cat MANIFEST`; do
    cp $afile $tools
done


