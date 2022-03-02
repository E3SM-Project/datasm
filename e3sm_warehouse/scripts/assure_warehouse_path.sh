#!/bin/bash

# Usage:  assure_warehouse_path <dataset_id> <source_root>
#
# This script expects positional inputs (1) dataset_id, and (2) source_root, where
#  source_root is usually "/p/user_pub/e3sm/e3sm_warehouse" or "/p/user_pub/work" (pub_root),
#  but can be any directory such that (root)/<dataset_id faceted path>/v# leads to the data.
#
# This script will return the given source_root if the files in the derived path are uniform,
#  Or else it will return a "holospace" substitute source root where the expected faceted path
#  leads to a directory of unified symlinks.
#
# This script will:
#  Find files in the given directory containing the substring pattern "nnnn-nn" (for YYYY-MM)
#  If no such file is found, prints ERROR message to stderr, and exits.
#  For the first file with matching pattern, sets "lead" to the preceding portion of the string.
#  Tests if ALL files begin with "lead". If so, it returns the supplied input source_root.
#  Otherwise, creates a holodeck dir, and for each file that matches the "nnnn-nn" pattern,
#  it tests if that file has a matching "lead".  If so, a symlink to the file is added to the
#  holodeck dir, If not, a symlink to the file with name modified to match the original "lead"
#  is added to the holodeck.  The holodeck equivalent of a warehouse_root is returned.

holospace="/p/user_pub/e3sm/staging/holospace/warehouse"

# Generate full source path to files from dataset_id and source_root
dataset_id=$1
sourceroot=$2
ds_part=`echo $dataset_id | tr . /`
ensemble_dir="$sourceroot/$ds_part"
vdir=`ls $ensemble_dir | tail -1`
sourcepath="$ensemble_dir/$vdir"

if [ ! -d $sourcepath ]; then
    echo "ERROR: Not a directory: $sourcepath" >&2
    exit 1
fi

echo "INFO: Using derived sourcepath: $sourcepath" >&2

# obtain the fixed "lead" of the first legitimate file
# determine if all files are uniform or a "lead" must be imposed

lead=""
got_lead=0

for afile in `ls $sourcepath`; do
    if [[ $afile =~ ([0-9]{4}-[0-9]{2}) ]]; then
        got_lead=1
        foundstring=${BASH_REMATCH[1]}  # part matching "YYYY-MM"
        rest=${afile#*$foundstring}     # everything after "YYYY-MM"
        spos=`echo $(( ${#afile} - ${#rest} - ${#foundstring} ))`
        lead=${afile:0:$spos}           # everything BEFORE "YYYY-MM"
        echo "INFO: Found Lead: $lead" >&2
        ppos=$((spos - 1))
        zcount=`ls $sourcepath | cut -c1-$ppos | uniq | wc -l`
        if [[ $zcount == 1 ]]; then
            echo "INFO: dir is uniform" >&2     # ALL files begin with "lead".
            echo $sourceroot
            exit 0
        fi
    fi
    if [[ $got_lead -eq 1 ]]; then
        break
    fi
done

if [[ $got_lead -eq 0 ]]; then
    echo "ERROR: no proper files in directory $sourcepath" >&2
    exit 1
fi

ts=`date -u +%Y%m%d_%H%M%S_%6N`
new_root="$holospace/holodeck-$ts"
new_path="$new_root/$ds_part/$vdir" 

mkdir -p $new_path

lc=0

for afile in `ls $sourcepath`; do
    lc=$((lc + 1))   
    if [[ $afile =~ ([0-9]{4}-[0-9]{2}) ]]; then
        foundstring=${BASH_REMATCH[1]}
        rest=${afile#*$foundstring}
        spos=`echo $(( ${#afile} - ${#rest} - ${#foundstring} ))`
        head=${afile:0:$spos}
        targ=`realpath $sourcepath/$afile`
        if [[ $head == $lead ]]; then
            # echo "RESULT: $afile"
            ln -s $targ $new_path/$afile
            continue
        fi
        tail=${afile:spos}
        # echo "RESULT: $lead$tail"
        ln -s $targ $new_path/$lead$tail
    else
        continue
    fi
done

echo "INFO: Produced holodeck source_path: $new_path" >&2
echo $new_root

exit 0

