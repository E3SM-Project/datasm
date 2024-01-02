#!/bin/bash

archman=`$DSM_GET_PATH ARCHIVE_MANAGEMENT`

pending=`ls $archman/extraction_requests_pending | wc -l`
deferred=`ls $archman/extraction_requests_deferred | wc -l`
processed=`ls $archman/extraction_requests_processed | wc -l`

echo "Extraction Requests:   Pending = $pending"
echo "Extraction Requests: Processed = $processed"
echo "Extraction Requests:  Deferred = $deferred"

echo ""
echo "Extraction queues are $archman/(extraction_requests_pending/, extraction_requests_processed/)"

holospace=`$DSM_GET_PATH DSM_STAGING`/holospace
last_holodeck=`ls -rt $holospace | tail -1`

echo "latest holodeck processing in $holospace/$last_holodeck"

