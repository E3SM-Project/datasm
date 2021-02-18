
BIG SELF_DEFINED TASK:

	Create a utility that will peruse EVERY archived directory (Experiment-Ensemble), to extract ALL tarred filepaths, to record the tar_paths to the actual files intended per (publish-able) dataset.
	e.g., for DECK,
		(atm nat mon) should return "archive/cam/hist/"  (for which the file_extraction_pattern *.cam.h0* will work beautifully)
	but, for HRv1,
		(atm nat mon) would return "run-0006-01-01-180907--0046-01-01-190111/"		(for years 0006-0045)
				       and "run/"						(for years 0046-0055)

	BOTH of the latter being still inadequate for a simple pattern like *cam.h0*, because those paths also archived other stuff matching *cam.h0*, so upon human inspection, we discover we need

			"run-0006-01-01-180907--0046-01-01-190111/theta"
		and	"run/theta"

	in order that the realm/freq pattern *cam.h0* will finally isolate the desired files.


THE GOAL:
	For any required publication of a [ CAMPAIGN MODEL EXPERIMENT ENSEMBLE (realm gridtype freq) ], we will have a table of (one or more) entries

		CAMPAIGN,MODEL,EXPERIMENT,ENSEMBLE,realm_gridtype_freq,archivePath,tarfilePath+extractionPattern

PREREQS:

	Ensure that archive/.cfg file "Standard_Datatype_Extraction_Patterns" are complete with respect to required publications


The Working Process:

0.  Upon arrival of new Archives, ensure that /p/user_pub/e3sm/archive/.cfg/Archive_Locator is updated.

1.  Issue ./archive_path_mapper_stage1.sh file_of_archive_specifications

	The "file_of_archive_specifications" can be the entire Archive_Locator (/p/user_pub/e3sm/archive/.cfg/Archive_Locator)
	or any file containing a subset of those lines, indicating the archives to be processed.

	Output will fill directory "PathsFound" with files of the form
		C:M:E:E:DSTITLE
	e.g.
		BGC:1_1:hist-BCRC:ens1:atm_nat_6hr

	Each file will contain all filenames found in archive using patterns found by the "tail" patterns in the_SDEP.

2.  Issue ./archive_path_mapper_stage2.sh 
	This will examine each file in PathsFound, trim away all lines that begin with known inappropriate path patterns
	and for the paths that remain, list the first and last files found for those patterns.
	Output wil be the file "headset12_lists_first_last", which will contain hundreds of entries of the form

		DECK:1_0:abrupt-4xCO2:ens1:lnd_nat_mon
		    HEADF:archive/lnd/hist/20180215.DECKv1b_abrupt4xCO2.ne30_oEC.edison.clm2.h0.0001-01.nc
		    HEADL:archive/lnd/hist/20180215.DECKv1b_abrupt4xCO2.ne30_oEC.edison.clm2.h0.0156-10.nc
		    HEADF:run/20180215.DECKv1b_abrupt4xCO2.ne30_oEC.edison.clm2.h0.0155-12.nc
		    HEADL:run/20180215.DECKv1b_abrupt4xCO2.ne30_oEC.edison.clm2.h0.0155-12.nc

	One must edit this file MANUALLY to determine which paths to the "First and Last" basenames is the correct one.
	In the case above, this would be "archive/lnd/hist", and since the "lnd_nat_mon" search pattern is *clm2.h0*,
	we need to convert that 5-line entry into

		DECK:1_0:abrupt-4xCO2:ens1:lnd_nat_mon:archive/lnd/hist/*clm2.h0*

	When such editing is completed, the finished file should be named "archive_dataset_map_prelim"

3.  Issue ./archive_path_mapper_stage3.sh
	This will take the output from stage 3, "archive_dataset_map_prelim", together with the Archive_Locator,
	to insert the appropriate archive_path into the final listing of the Archive_Map.


EXTRA_CREDIT:

        4.  Ensure the file "/p/user_pub/e3sm/archive/.cfg/Filename_SimDate_Locator" has an entry
            for each experiment dataset, indicating the "dot"-delimited position of the sim-date
            value in each filename.  Use the file "headset12_lists_first_last" to obtain the information.


