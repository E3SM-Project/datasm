
1.  [optional] Use 3_0_trim_dsids_to_found_in_archive_map.sh to limit candidate dataset_ids to only those
    that were discovered in archive_mapping.

2.  Call:  3_1_run_extraction.sh <to_extract_dsid_list> [prestage] [ archmap=<pathToArchiveMap> ]
        (The parameters will be passed to [STAGING_TOOLS]/datasm_extract_from_archive.sh)

    This will create an "extraction_request" file for each listed dataset_id into "pending":
                [STAGING_RESOURCE]/archive/extraction_requests_pending/    (immediate extraction queue)

    If argument "prestage" appears, the extraction requests are placed into "prestage":
                [STAGING_RESOURCE]/archive/extraction_requests_prestage/   (extraction deferred)

    The "prestage" allows the user to inspect the tickets, and add them manually to "pending" in a given time-order.
    NOTE: The tickets must be COPIED, not MOVED, if they are to obtain the desired time-of-arrival ordering.

    (The archive_extraction_service will be started automatically, if not already running)

3.  Once extraction is completed, run the 3_2_report_excess and 3_3_remove_excess scripts to trim excess years/files. 


NOTE:  To check current status, use

    check_extraction_queues.sh          [see if any requests are still pending, locate processed requests, etc]
    
        checks: [STAGING_RESOURCE]/archive/extraction_requests_pending/
                [STAGING_RESOURCE]/archive/extraction_requests_processed/
                [STAGING_RESOURCE]/archive/extraction_requests_deferred/

    For detailed current processing, examine [STAGING]/holospace/holodeck-<date>/

IMPORTANT:
    Once extraction is completed, run the 3_2_report_excess and 3_3_remove_excess scripts to trim excess years/files 
    PRIOR to validation and other stages of data processing.

