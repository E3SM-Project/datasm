#!/bin/bash

note='Usage: archive_extraction_ticket.sh nat_dsid [archmap=<path_to_archive_map>]'

note='This routine accepts a single native dataset_id, and an options (alternate) Arvhive_Map,'
note='and creates a ticket named "extraction_request-<dataset_id>" whose lines contain those'
note='lines from the Archive_Map holding the extraction parameters for the given dataset_id.'
note='Multiple lines are possible, including mutpliple archives for a single dataset_id.'

archman=`$DSM_GETPATH ARCHIVE_MANAGEMENT`
tools=`$DSM_GETPATH STAGING_TOOLS`
users=`$DSM_GETPATH USER_ROOT`

if [[ $# -eq 0 ]]; then
    echo "Usage: archive_extraction_ticket.sh <nat_dsid> [archmap=<path_to_archive_map>]"
    exit 0
fi

arch_map="$archman/Archive_Map"

nat_dsid=$1

i=1
while [ $i -le $# ]; do
    if [[ ${!i:0:8} == "archmap=" ]]; then
        arch_map=${!i:8}
        echo ARCH_MAP=$arch_map
    fi
    ((++i))
done

# echo "Archive_Extraction: Operating with Archive_Map = $arch_map"
# echo "Archive_Extraction: Seeking content for dsid = $nat_dsid"

ticketname="extraction_request-${nat_dsid}"

cat $arch_map | fgrep ",${nat_dsid}," > $ticketname

exit 0


