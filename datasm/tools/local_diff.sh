#!/bin/bash

# must execute in tools directory

resource=`$DSM_GETPATH STAGING_RESOURCE`
arch_man=`$DSM_GETPATH ARCHIVE_MANAGEMENT`
tools=`$DSM_GETPATH STAGING_TOOLS`

echo "======================================================================================================"
echo "dataset_spec.yaml:"
diff ../resources/dataset_spec.yaml $resource/dataset_spec.yaml

echo "======================================================================================================"
echo "derivatives.conf:"
diff ../resources/derivatives.conf $resource/derivatives.conf

echo "======================================================================================================"
echo "Archive_Locator:"
diff ../resources/Archive_Locator $arch_man/Archive_Locator

echo "======================================================================================================"
echo "Archive_Map:"
diff ../resources/Archive_Map $arch_man/Archive_Map

echo "======================================================================================================"
echo "Archive_Map_headers:"
diff ../resources/Archive_Map_headers $arch_man/Archive_Map_headers

echo "======================================================================================================"
echo "Standard_Datatype_Extraction_Patterns:"
diff ../resources/Standard_Datatype_Extraction_Patterns $arch_man/Standard_Datatype_Extraction_Patterns

echo "######################################################################################################"
echo "TOOL INSTALLATIONS"

for atool in `cat MANIFEST`; do
    echo "======================================================================================================"
    echo "$atool:"
    btool=`basename $atool`
    diff $atool $tools/$btool
done


