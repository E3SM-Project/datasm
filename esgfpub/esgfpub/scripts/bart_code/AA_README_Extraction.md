The tools within Pub_Work/1_Refactor begin with properly extracting desired dataset files from archive,
whether for immediate push to publication or for push to "Staging" where possible regridding or other
data cleanup may be applied.

NOTE:  One or more lines selected from the Archive_Map (/p/user_pub/e3sm/archive/.cfg/Archive_Map)
	will be required in a temporary file as an input parameter.  Beware that some datasets are
	spread across two different archives, and both lines may be required in order to extract
	a complete dataset.

A.  To list or extract files withough the creation of the full-faceted publication directories,
    a simple script is supplied:

	extract_dataset_files_to.sh  file_with_one_line_from_Archive_Map [destination_directory]

    If no destination directory is given, only the list of files that would have been extracted are
    provided (streamed to stdout).  Otherwise, the files are extracted to the indicated destination.

B.  If one or more datasets are to be extracted for publication or staging, with the consequent
    construction of faceted publications (or staging) directories, the script

	publication_staging_control_script.sh  configfile  file_of_lines_from_Archive_Map

    will automate most of that process.  In particular, you can publish/pre-publish multiple
    dataset frequencies in a single pass.  The necessary steps are

    1.  Create the config file.

    IMPORTANT:  Due to values hard-coded into the jobset_config file, one must invoke this "automated" process
    for each combination of (realm,freq_set,resolution,dataset_version).  The jobset_config file must
    contain:

	dstype_static=<type>    (where <type> examples are "atm nat", "ocn nat", "lnd nat", etc)
	dstype_freq_list=<list> (where <list> examples are "mon", "day 6hr 6hr_snap", etc)
	resolution=<res>        (where res is one of 1deg_atm_60-30km_ocean or 0_25deg_atm_18-6km_ocean)
	pubvers=<ver>           (where ver is one of v1, v2, etc)
	overwriteFlag=<0|1>     (Boolean, allows adding files to a non-empty destination directory)

	As we are extracting from archives, the first variable will always be "<realm> nat".
	The second can be any space-separated set of frequency-codes.
	See the file: /p/user_pub/e3sm/archive/.cfg/Standard_Datatype_Extraction_Patterns for options.


    2.  Create a file "the_target_archives" consisting of lines selected from the file

	/p/user_pub/e3sm/archive/.cfg/Archive_Locator

	The lines listed must cover all cases (experiment,ensembles) for all intended datasets 
	that are consistent with the values in the config file.


    3.  Invoke

	nohup ./publication_staging_control_script.sh the_target_archives &

	A log file "Holodeck_Process_Log-<date_time>" will be produced to track progress.

    If datasets for multiple realms or resolutions is required, you will need to repeat Steps 1,2,3 
    to reflect a different realm or resolution, and likewise select an archive list appropriately.








