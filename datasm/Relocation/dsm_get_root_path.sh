#!/bin/bash

# This one path, "reloc_root" must be hard-coded per site installation.
# It must contain this very script ".dsm_get_root_path.sh" and the file
# giving the root_tag:root_path values, ".dsm_root_paths"

reloc_root=/p/user_pub/e3sm/staging

if [ $# -lt 1 ]; then
    echo "ERROR: No path tag specified from $reloc_root/.dsm_root_paths"
    exit 1
fi

tag=$1

if [ $tag == "ALL" ]; then
    cat $reloc_root/.dsm_root_paths
    exit 0
fi

grep ^$tag $reloc_root/.dsm_root_paths | cut -f2 -d:


