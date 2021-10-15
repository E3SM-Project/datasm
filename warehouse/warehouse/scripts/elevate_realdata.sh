#!/bin/bash

src_edir=$1

# find highest version directory vX in edir, create next version dir vY.

# for afile in vX, obtain realpath and MOVE to the target to vY

# conduct unify_filenames_post_validation on vY

src_vdir=`ls $src_edir | tail -1`
src_vdir=$src_edir/$src_vdir

body=`dirname $src_vdir`
leaf=`basename $src_vdir`
veepart=${leaf:0:1}
numpart=${leaf:1}

if [ ! $veepart == "v" ]; then
    echo "BAD leafdir $leaf"
    exit 1
fi

if ! [[ ${numpart:0:1} =~ [0-9] ]]; then
    echo "BAD leafdir $leaf"
    exit 1
fi

newnump=`echo "x=$numpart + 0.1; if(x<1) print 0; x" | bc`

dst_vdir="$body/${veepart}$newnump"

mkdir -p $dst_vdir

for afile in `ls $src_vdir`; do
    rfile=`realpath $src_vdir/$afile`
    echo "mv $rfile $dst_vdir"
    mv $rfile $dst_vdir
done



