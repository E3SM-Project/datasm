#!/bin/bash
#!/bin/bash

dsidlist=$1
reptag=`basename $dsidlist`

statpath=`$DSM_GETPATH STAGING_STATUS`

rep_name="last_stat_$reptag"

rm -f $rep_name

for dsid in `cat $dsidlist`; do
    sf=${dsid}.status
    stat_val=`tail -1 $statpath/$sf`
    echo "$dsid:$stat_val" >> $rep_name
done

echo "Completed $rep_name"

