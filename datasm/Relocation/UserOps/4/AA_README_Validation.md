Conducting native dataset validation:
====================================

Create a list of native dataset_ids to be validated, and place into an "Ops" subdirectory.
"cd" into that directory and issue

    nohup ../run_validation.sh <dsid_list> [<alt_dataset_spec.yaml>] &

This will call [STAGING_TOOLS]/run_datasm_LocalEnv_validation_dsid_list_serially.sh <dsidlist> [spec=<alt_dataset_spec.yaml>]
(A "Validation_log" file will be created automatically.)

NOTE: The dsidlist need only contain "native" "model-output" dataset_ids to be validated, as we have
no mechanism for validating namefiles, restarts, or "climos" or "time-series".  Use the global

    [STAGING_TOOLS]/list_e3sm_dsids.py [-d <alternate_dataset_spec.yaml>]

to generate your list of dataset_ids, and filter down with

    " | grep <model_version> | grep <experiment> ... | grep native | grep model-output "

A good habit is to name the dsidlist "to_validate_<model_experiment_etc>_<number_of_datasets>".

As each dataset_id is processed, a Validation_Log and a slurm_scripts directory will be produced.

    The log will be named
        Validation_Log-<timestamp>-<dataset_id>

    The "slurm_scripts" directory will be renamed upon completion to
        slurm_scripts-<timestamp>-<dataset_id>

Upon completion, you can use

    grep "is in state" Validation_log-* 

to see which validations "Pass" or "Fail".

For process management, you can then divide the dsidlist into those that Passed and those that Failed, as

    "success_validate_<model_experiment_etc>_<number_of_datasets_passed>" and
    "to_validate_<model_experiment_etc>_remaining_<number_of_datasets_failed>"

and explore the slurm_scripts directories of the failures for their slurm logs to learn the reason for failures.

For long-term record-keeping, issue: "../zclean.sh".  This will move the completed Validation_Logs and the
renamed slurm_directories into

    [Operations]/4_Dataset_Validation/validation_logs/
    [Operations]/4_Dataset_Validation/slurm_history/

You can always locate a given validation log or slurm_scripts directory by performing

    ls <directory> | grep <dataset_id>

since the dataset_id is part of the name of the stored logfile or slurm_scripts directory.


========================================================================================================== 


HELP with individual validation modules:
 
======== help: checkUnits       ========================================
 
usage: checkUnits.py [-h] [--time-name TIME_NAME] [-q] [-p PROCESSES] input

Check that the time units match for every file in the dataset

positional arguments:
  input                 Path to a directory containing a single dataset

optional arguments:
  -h, --help            show this help message and exit
  --time-name TIME_NAME
                        The name of the time axis, default is 'time'
  -q, --quiet           suppress status bars and console output
  -p PROCESSES, --processes PROCESSES
                        number of parallel processes
 
======== help: checkTime        ========================================
 
usage: checkTime.py [-h] [-j JOBS] [-q] input

Check a directory of raw E3SM time-slice files for discontinuities in the time
index

positional arguments:
  input                 Directory path containing dataset

optional arguments:
  -h, --help            show this help message and exit
  -j JOBS, --jobs JOBS  the number of processes, default is 8
  -q, --quiet           Disable progress-bar for batch/background processing
 
======== help: Done.            ========================================
