The utilities or tools contained herein are not strictly necessary for datasm operation,
and are not called upon by the datasm system.  They are nonetheless essential for user
management of ancillary datasm operation, status determination, and related tasks that
support E3SM data operations.  Their functions are briefly described below:

NOTE:  These tools are "installed" to /p/user_pub/e3sm/staging/tools, and many rely upon
definition files (Archive_Map, dataset_spec.yam) in /p/user_pub/e3sm/staging/resource.

NOTE:  To employ these tools, you must add the following to your .bashrc file:

    export DSM_GETPATH=/p/user_pub/e3sm/staging/.dsm_get_root_path.sh

    Many of these scripts call upon other scripts, and rather than have the paths to
    these scripts hard-coded into each script (making then non-relocatable), the above
    "dsm_get_root_path.sh" reads a table (/p/user_pub/e3sm/staging/.dsm_root_paths)
    to resolve keywords used to specify important datasm root paths.

NOTE:  Much of E3SM data operations involve "datasets", and in that regard, dataset_ids
are ubiquitous and employed as "tokens" for many operations.  Single dataset_ids, or
files containing lists of dataset_ids, are often among the parameters given to a tool to
accomplish a given task.  Hence the utilities "list_e3sm_dsids" and "list_cmip_dsids",
each of which employ the dataset-defining "dataset_spec.yaml" are often filtered down to
an appropriate list of dataset_ids as a first step in condicting operations.



IMPORTANT CONFIGURATION FILES:

    /p/user_pub/e3sm/staging/.dsm_root_paths
    /p/user_pub/e3sm/archive/.cfg/Archive_Locator
    /p/user_pub/e3sm/archive/.cfg/Archive_Map
    /p/user_pub/e3sm/archive/.cfg/Standard_Datatype_Extraction_Patterns
    /p/user_pub/e3sm/staging/resource/dataset_spec.yaml
    /p/user_pub/e3sm/staging/resource/table_cmip_var_to_e3sm

ALPHABETIC LISTING OF TOOLS:

archive_dataset_extractor.sh:

    archive_dataset_extractor.sh infile [dest_directory]

    Accepts a file of ONE line from the Archive_Map, and will extract the corresponding
    datasets to the dest_directory (or just list them, if no directory is given.)

archive_dataset_extractor.py:

    python archive_dataset_extractor -a am_specfile [-d dest_dir] [-O]

    Similar to the bash script "archive_dataset_extractor.sh".  Use "--help" for details

archive_extraction_service.py:

    nohup python archive_extraction_service.py &

    Runs much as a background daemon to service "extraction request tickets" placed in

        /p/user_pub/e3sm/archive/.extraction_requests_pending/
        /p/user_pub/e3sm/archive/.extraction_requests_processed/  (moved here when done)

    These tickets are automatically generated by "datasm_extract_from_archive.sh",
    which is designed to restart the archive_extraction_service if it is not running.
    Corresponding datasets are extracted to the warehouse (/p/user_pub/e3sm/warehouse).

archive_path_mapper.py

    archive_path_mapper -a al_listfile [-s sdepfile]

    Accepts a selected list of lines (from /p/user_pub/e3sm/archive/.cfg/Archive_Locator)
    and for each line, creates a cross-product with each datatype matching pattern found
    in the (archive/.cfg/) Standard_Datatype_Extraction_Patterns (or a supplied subset).
    Each pattern is applied to the indicated zstash archive to produce all fund lists of
    pattern-matched files.  A body of preliminary "Archive_Map" lines are generated, each
    of which must be reviewed and manually completed in order to upda the Archive_Map.

arch_loc_from_arch_map.sh

    arch_loc_from_arch_map.sh am_lines

    Accepts a selection of Archive_Map lines, and gives the set of Archive_Locator lines
    that correspond The dataset types (realm, grid, frequency) are lost.  Good for the
    setup of "zstash holodeck" symlinks in the manual exploration of archives.

consolidated_cmip_dataset_report.py
consolidated_e3sm_dataset_report.py

    These generally take no arguments, and survey the Archive_Map, dataset_spec.yaml,
    the warehouse filesystem, the publication filesystem, and the esgf search nodes in
    order to determine and report the status of each dataset that exists under the E3SM.
    The output is CSV, and (with a little work) can produce an Excel Spreadsheet that
    can be sorted on each of 20+ fields.

contract_dataset_spec.py

    contract_dataset_spec -i expanded_dataset_spec -o contracted_dataset_spec

    Accepts a standard form of the dataset_spec (aka "expanded") and will take each
    dataset specification "tree", isolate the "case_extensions" branch (where the
    resolution-specific dataset reams and frequencies are specified), and collect
    these to create a consolidated unique "CASE_EXTENSIONS" tree, reducing each
    dataset_tree to its global parameters plus a "Case_Extension_ID" in place of the
    original extensions.

    This makes it much easier to edit the branches (reduced form about 60 to 12) and
    easier to compare and contrast the major dataset definitions, and news ones, etc.
    See "expand_dataset_spec.py" for the opposite transform.

datasm_extract_from_archive.sh

    datasm_extract_from_archive.sh <file of native dataset_ids>

    This script will create "archive extraction request tickets" that can be read
    by the "archive_extraction_service" to pull datasets from archive to warehouse.

    This routine is a stand-in for "datasm extract", which has yet to be coded into
    the Data State Machine as a built-in feature.

datasm_pp_sourceroot.sh

    Given a dataset_id (E3SM or CMIP6), this script will return the root of the path
    to the best (latest-version) of the corresponding dataset, of else return "NONE".
    If found, it will return "/p/user_pub/e3sm/warehouse" or "/p/user_pub/work",
    whichever has the latest data.  This utility is a "temporary fix" for some
    datasm operation command-line elements, to override the "smart but incorrect"
    internal behavior regarding the desired source and destination of generated data.

datasm_verify_publication.py

    datasm_verify_publication -i listfile_of_dsids [-u | --update-status]

    Given a file containing one or more dataset_ids (E3SM or CMIP6), this utility will
    contact the ESGF search server and return the publication status of each dataset.
    If published, it will check also that the latest published version matches the
    latest dated version in the /p/user_pub/work publication directory.  It will return
    either
        <dataset_id>:PUBLICATION:Verified
    or  <dataset_id>:PUBLICATION:Verification_Fail:<reason>

    if [-u | --update-status] is specified, the corresponding status file for the
    given dataset if updated to reflect this status, unless the dataset is not found
    in the publication directory.

dsids_to_archive_map_keys.sh

    Given a file containing one or more native dataset_ids, this script will return
    the set of key strings identifying the Archive_Map lines for these datasets.

dsids_to_archive_map_lines.sh

    Given a file containing one or more native dataset_ids, this script will return
    the set of Archive_Map entries enabling the archive extraction of the datasets.

ds_paths_info_dsid_list_compact.sh

    Given a file containing a list of dataset_ids, this script will output for each
    dataset_id the values given by "ds_paths_info.sh", with the additional benefit
    that each version directory and file count is listed parenthetically on the same
    line as the corresponding warehouse or publication path.

ds_paths_info_dsid_list.sh

    Given a file containing a list of dataset_ids, this script will output for each
    dataset_id the values given by "ds_paths_info.sh"

ds_paths_info.sh

    Given a dataset_id (E3SM or CMIP6), this script will:
    a.  Print the full path to the dataset's ststus file, if it exists.
    b.  Print the last status value in the status file, if it exists.
    c.  Print the full path to warehouse ensemble directory, and below it
        the list of versions and their file counts.
    d.  Print the full path to publication ensemble directory, and below it
        the list of versions and their file counts.

    This provide a very rapid way to confirm the status of archive extraction, of
    dataset validation, of publication, and of the generation of derivative climos,
    time-series and CMIP6 variable datasets.

    One may select any of the components above by seeking the keywords SF_PATH,
    STATUS, WH_PATH, or PB_PATH.

ensure_status_file_for_dsid.sh

    For any given dataset "dsid", this script will return the full path to the status
    file for the corresponding dataset (/p/user_pub/e3sm/staging/status/<dsid>.status)
    If no status file exists, one is created.  The status file is used by the DataSM
    both to record the history of processing conducted with respect to the dataset,
    and to provide the "latest state", necessary for the Data State Machine to advance
    required processing.

expand_dataset_spec.py

    expand_dataset_spec -i contracted_dataset_spec -o expanded_dataset_spec

    This utility will turn a contracted form of the E3SM dataset_spec.yaml file
    back its original expanded form, by replacing each "Case_Extension_ID" with
    the actual case extension branch found in the CASE_EXTENSIONS tree of the
    contracted specification.  See "contract_dataset_spec.py" for details.

get_e3sm_vars_for_cmip.sh

    Given a CMIP6 variable as input, this script will output the corresponding E3SM
    variable (or CSV list of variables) required in the calculation of the CMIP6
    variable.  The output is obtained by running and parsing "e3sm_to_cmip --info"

latest_data_location_by_dsid.sh

    For a given input dataset_id, will scour both the warehouse and publication
    filesystems for the latest populated version directory.  The full path to the
    best location is returned, or else "NONE" if not populated directory is found.

list_cmip6_dsids.py
list_e3sm_dsids.py

    These take no parametersr.  They will output ALL CMIP6 or E3SM dataset_ids,
    employing the currently installed dataset_spec.yaml as source.  The results
    may then be filtered with "grep" or "cut" for many applications.

metadata_version.py

    Usage:  -i <full_path_to_metadata.json file> --mode [get|set]
    If mode is "get", the value of the variable "version" (or "NONE") is returned.
    If mode is "set", then "version" is set to vYYYYMMDD (current UTC date).

parent_native_dsid.sh

    Given a CMIP6 dataset_id, the native (E3SM) dataset_id corresponding to the
    source data is returned.

report_first_file_for_latest_on_dsid_list.sh

    Employing "latest_data_location_by_dsid.sh" (above), this script will take
    any E3SM or CMIP6 dataset_id, and return the full path to the first data file
    found in the dataset (or else "NONE").

restart_services.sh

    Ensures the archive extraction service is running to service requests.
    Is called automatically by datasm_extract_from_archive.sh
 
rw_yaml.py

    rw_yaml -i yaml_in -o yaml_out [-s] [-t tabsize]

    This utility will read an arbitrary yaml-format file as a python "dictionary"
    of arbitrary depth for processing, and then write the dictionary out to yaml
    file format.  Optionally, dictionary entries are sorted, and the tab-size may
    be changed.  Blank lines and comments are lost.

tell_years_dsid.py

    For the given dataset_id, consults the dataset_spec.yaml file to report the
    official "start_year,end_year" for dataset publication.

trisect.py

    usage:  trisect <listfile1> <listfile2>

    Given two files, "F1" and "F2", each assumed to be lists of items (files,
    dataset_ids, variables, etc), this routine will output 3 files:

        only-F1:        items found only in list F1
        only-F2:        items found only in list F2
        both-F1_and_f2: items common to both lists
