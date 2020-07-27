The Nearly Automated Dataset Extraction and (Pre)Publication Process

TL;DR:
	IMPORTANT:  Due to values hard-coded into the scripts, one must invoke this "automated" process
		for each combination of (realm,freq_set,resolution,dataset_version)

	1.  Create a file "the_target_archives" consisting of lines selected from the file

		/p/user_pub/e3sm/archive/.cfg/Archive_Locator

		The lines listed must cover all cases (experiment,ensembles) for all intended datasets 

	2.  Edit the "publication_staging_control_script.sh" to set the values for these two variables

		dstype_static="atm nat"	(EXAMPLE)
		dstype_freq_list="mon day 6hr_snap day_cosp" (EXAMPLE)

		As we are extracting from archives, the first variable will always be "<realm> nat".
		The second can be any space-separated set of frequency-codes.
		See the file: /p/user_pub/e3sm/archive/.cfg/Standard_Datatype_Extraction_Patterns for options.

	3.  Edit the "holodeck_stage_publication.sh" script to set the values for these two variables

		resolution=1deg_atm_60-30km_ocean
		pubvers=v1

	4.  Invoke

		nohup ./publication_staging_control_script.sh the_target_archives &

		A log file "Holodeck_Process_Log-<date_time>" will be produced to track progress.

	If datasets for multiple realms is required, you will need to repeat Step 2 to reflect a different realm,
	and re-invoke Step 3.


BACKGROUND:


E3SM Publication Staging is now operating under a new premise:  All data archives are now local and amenable to zstash extraction.

I have crafted a working - if fragile - automated extraction regime that can access multiple experiments and cases, locate their archive directories and file extraction patterns, create the faceted publication destination directories, and successfully extract and move the dataset files to the intended publication directories.  I would caution that the process is not yet entirely generalized, and should be reimplemented at some point in a fixed python application, once the dust settles on a stable procedure.

The following provides background and detailed explanation of the process.  As I write this, it is already staging all of the DECK cam.h[1-4] datasets, for all experiments previously published.

BACKGROUND:  File Directory Structure and Configurations

The LLNL E3SM Archives presently have the following location and structure:

	/p/user_pub/e3sm/archive/
				1_0/
					DECK/
						(case_leaf)/tarfiles + index.db
						(case_leaf)/tarfiles + index.db
						...
					HiResMIP/
						(case_leaf)/tarfiles + index.db
						(case_leaf)/tarfiles + index.db
						...
				1_1/
					BGC/
						(ditto)
				1_1_ECA/
					BGC/
						(ditto)

	Note that "case_leaf" is a directory name uniquly identifying an experiment and ensemble.
	EXAMPLE: 20180622.DECKv1b_A2_1850aeroF.ne30_oEC.edison  (here, A2 = AMIP ens2)
	These directory names exactly match the author-supplied NERSC HPSS leaf directory names.
	This practice facilitates point-to-point Globus transfers.

	Ideally, the "case-leaf" directory should contain nothing but the tar files and the zstash database file "index.db".  In practice, it may contain other material, but no further directory traversal should be required to locate the available tar files.

In order to provide the opportunity for various "staging" dataset checks or modifications, the files are extracted to 

	/p/user_pub/e3sm/staging/pre_pub/(facets)/(files.nc)

where (facets)/(files.nc) matches the structure of the eventual publication directories.

We can extract and move the dataset files to publication using zstash, while leaving the archive intact and clean, by employing a "holodeck" of symlinks. (Credit to Sterling Baldwin for schooling me on the virtues of symlinks. I merely take credit for the use of the term "holodeck" in this context.)

With any empty directory serving as the "holodeck", the following depicts the sequence of operations leading to extraction and movement of dataset files to their intended publication directory:

1. Begin with empty directory, call this the $HolodeckPath

	/path/to/holodeck/

2. Create a zstash subdirectory

	/path/to/holodeck/
			zstash/

3. Having located the appropriate "case-leaf" archive directory, form a list of the contained tar file names (and index.db) and their full paths, and loop over each to create in the zstash subdirectory the necessary symlinks

	/path/to/holodeck/
			zstash/
				tarfilename --> symlink to full tarfile path
				tarfilename --> symlink to full tarfile path
				...
				index.db --> symlink to full index.db path

4. With the holodeck as the working directory, and the necessary datafile extraction pattern in hand, one can issue the zstash extract command to obtain

	/path/to/holodeck/
			stored/path/to/
				datafile
				datafile
				...
				datafile

			zstash/
				tarfilename --> symlink to full tarfile path
				tarfilename --> symlink to full tarfile path
				...
				index.db --> symlink to full index.db path

5. Having previously constructed the staging/prepub/(facets)/ destination path (targ_pub_dir="$prepubroot/$modelver/$exp_name/$midpath/$freq/$ensemble/$pubvers") and ensured it is empty (may be overridden with "overwriteFlag=1", and cognizant of the extracted tarpath-head (e.g. "stored/path/to/"), one can now issue

	mv stored/path/to/* pub_dest_path
	chmod 644 pub_dest_path/*

   leaving you

	/path/to/holodeck/
			stored/path/to/
				(empty)

			zstash/
				tarfilename --> symlink to full tarfile path
				tarfilename --> symlink to full tarfile path
				...
				index.db --> symlink to full index.db path

6. At this point, the dataset has been fully staged for pre-publication inspection and possible modifications.

   If there are additional datasets to publish from this same archive "case-leaf", one need only issue

	rm -rf $HolodeckPath/stored/

   and repeat steps 4 and 5 with another datafile_extraction_pattern, with working directory the $HolodeckPath with existing zstash symlinks.  

   If instead there are other cases for which datasets exist to be published, issue

	rm -rf $HolodeckPath/*

   and repeat from step 2 with a new case-leaf and associated datafile_extraction_patterns



IN SUMMARY:

With the "holodeck" as the working directory, a script given the desired (model,campaign,case,ensemble,datasetSpec) will need to

	1. Construct and test for existence and population of the target pre-publication directory.
		- Skip if the directory exists and is non-empty, unless the script overwriteFlag=1.
	2. Access the Archive_Map to obtain the information regarding the archive source data
		- Ensure exactly 1 line returned from CMEE_archive_map and CMEE_archive_map_augment
	3. populate the subordinate "zstash" subdirectory with simlinks to the appropriate tarfiles and index.db file.
		- Ensure holodeck contains only empty zstash subdirectory
	4. execute the zstash extraction command, with the appropriate file-selection pattern.
	5. create the necessary target publication directory
	6. move the extracted files to their publication directory
	7. clean the holodeck by removing all zstash symlinks and non-zstash directories

STEP 0:

	must contruct and test $pub_head_dir/$thisModel/$thisExp/$path_mid/$freq/$ens/v1
	where
		pub_head_dir=/p/user_pub/work/E3SM
		thisModel (supplied)
		thisExp   (supplied)
		path_mid  (supplied)	e.g. 1deg_atm_60-30km_ocean/atmos/native/model-output
		freq      (supplied)
		ens       (supplied)
		ver       (supplied)
