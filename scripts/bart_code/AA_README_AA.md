
We can now employ the archive/.cfg/Archive_Map to location and "ls" all files from all archives
for all experiments and dataset types, by appropriate pattern.

The archive assessment intends to determine, for each experiment[ensemble] and dataset type,
the count of matching files, and that the "first" and "last" file found in each matched set
are indeed from the same "set" (have the same name structure) and have rational simulated
date ranges (e.g. 1850_01 to 2014_12).

Files returned have the general structure:

	tar_path/structured_name.sim_date.nc

Because we are using the Archive Map as our key, we know that the "tar_paths" will match for
any first and last file found.  But we need to ensure that "structured_name" is a match, and
that the first and last "sim_dates" are reasonable. This presents two problems:

  1.  The beginning of the structured_name may have a generation-date that varies across the
      dataset, and must be trimmed off if we are to compare the datafile names for matches.

  2.  The structured_name has "dot-delimited" components, and the number of components will
      vary across dataset types, and even for the same dataset type across experiments. This
      makes locating the "sim-year" field often specific to each dataset, and we have created
      a table
		/p/user_pub/e3sm/archive/.cfg/Filename_SimDate_Locator

      that identifies the dot-delimited field for sim-year, given experiment and dataset type


For each "Campaign,Model,Experiment,Ensemble,DatasetType,Count,First,Last", we want to output

either
	Campaign,Model,Experiment,Ensemble,DatasetType,MATCHED,Count,firstDate,LastDate,years_total

or	Campaign,Model,Experiment,Ensemble,DatasetType,NOMATCH,Count,firstStructuredName,LastStructuredName

Anywhere a NO_MATCH occurs, we must modify the Archive_Map to provide more specificity to the entry.


THE PROCESS:

	1.  Ensure the file "/p/user_pub/e3sm/archive/.cfg/Filename_SimDate_Locator" has an entry
	    for each experiment dataset, indicating the "dot"-delimited position of the sim-date
	    value in each filename.  This should have been done as part of the ArchivePathMapper.

	2.  Issue

		archive_assess_count_first_last.sh  any_subset_of_Archive_Map_lines

	    If output contains spurious NO_MATCH indications, but only due to embedded machine
	    name in the file names, you can manually set the flag "trust_match=1" in the script.


