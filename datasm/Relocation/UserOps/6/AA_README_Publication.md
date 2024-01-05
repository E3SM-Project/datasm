
PROCEDURES FOR PUBLICATION
==========================

TO PUBLISH: Select an "Ops" subdirectory for your publication operation, and cd into it.
Create a file_list of dataset_ids, "to_publish_<description>_<count>", and ensure there is also
one (empty) file in the directory named "target-<count>", and with matching dataset_id count.

NOTE:  All dataset_ids in the "to_publish" list must come from the same dataset_spec.yaml file.
If not, you will need to split the list into separate lists and command-lines.

Launch the publication with:

    nohup ../run_publisher.sh <list_of_dsids_to_publish> [<alternative_dataset_spec.yaml>] &

        (This effects "datasm publish" through a STAGING_TOOLS script.)
        Run "../assess_ops.sh" to monitor progress of all "Ops" jobs.

        When no jobs remain (or progress seems halted and "ps gx" shows nothing running),
        run "../report_status.sh" to produce a list of dataset_ids that succeeded or failed to publish.
        The files will be named "Process_Successes_Ops<n>_<count>" and "Process_Failures_Ops<n>_<count>".

    Examine the "Failures" to determine the cause (review their Publication_Log(s) and slurm_scripts
    directories - each will contain the dataset_id in the name).

TO RETRY publication with subsets of Failed dataset_ids that appear to have failed for transient reasons
(server unavailable, NFS-mount lost (cannot find acme1, etc), produce a new listing.  A good naming
convention is match the original list name:

    "to_publish_<description>_<count>"

with

    "to_publish_<description>_<count>_rem_<remcount>"

Move the file "target-<count>" to reflect "<target-<remcount>" so that "assess_ops,sh" can monitor.

NOTE: Before re-issuing "..\run_publisher.sh", check to see if the files for any failed dataset were
already moved to the publication area of the filesystem.  To do this, run

    [STAGING_TOOLS]/ds_paths_info_dsid_list_compact.sh <to_publish_list>

and examine the WH_PATH and PB_PATH lines.  If any datasets have had their files moved to PB_PATH,
provide a list of those as ("to_reset"), and run:

    ../reset_publication_dsid_list.sh <to_reset>

Then proceed to launch publication as before.

 
FINALLY Issue:
    ../run_verify.sh <list_of_dsids_to_publish> [updatestatus] > verification_log

        (use [updatestatus] to have dataset status files reflect the verification)

Once satisfied with completion of operations, issue

    ../zclean.sh

        (archives publication_logs and slurm_script directories)

    Rename "to_publish" lists as "success_publish" lists where successful, etc.





(ancient text below)

Once extraction (and possibly validation and/or postprocess) completed, ensure

    PUBLICATION:Unblocked
    PUBLICATION:Ready

are set.  "Ready" should follow setting the warehouse version dir to v1 or greater.  The warehouse assign should apply

    python warehouse_assign.py --setdir-nextpub -w listfile_of_warehouse_dirs
    python warehouse_assign.py --setstatus PUBLICATION:Unblocked -w listfile_of_warehouse_dirs
    python warehouse_assign.py --setstatus PUBLICATION:Ready -w listfile_of_warehouse_dirs

Cleanly, these would be followed by

    python warehouse_publish --childspec PUBLICATION:PUB_PUSH --enslist listfile_of_warehouse_dirs
    python warehouse_publish --childspec MAPFILE_GEN --enslist listfile_of_warehouse_dirs
    python warehouse_publish --childspec PUBLICATION:PUB_COMMIT --enslist listfile_of_warehouse_dirs

