#!/bin/bash

# must execute in tools directory

resource=`$DSM_GETPATH STAGING_RESOURCE`
arch_man=`$DSM_GETPATH ARCHIVE_MANAGEMENT`
tools=`$DSM_GETPATH STAGING_TOOLS`

cp ../resources/dataset_spec.yaml $resource/dataset_spec.yaml
cp ../resources/derivatives.conf $resource/derivatives.conf
cp ../resources/Archive_Locator $arch_man/Archive_Locator
cp ../resources/Archive_Map $arch_man/Archive_Map
cp ../resources/Archive_Map_headers $arch_man/Archive_Map_headers
cp ../resources/NERSC_Archive_Map $arch_man/NERSC_Archive_Map
cp ../resources/Standard_Datatype_Extraction_Patterns $arch_man/Standard_Datatype_Extraction_Patterns

for afile in `cat MANIFEST`; do
    cp $afile $tools
done


