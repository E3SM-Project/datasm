To create mapfiles, ensure

	conda activate pub

In order to facilitate the generation of mapfiles for multiple data sets, the script

	multi_mapfile_generate.sh

is provided.  It takes as input a control file containing lines of one of three forms:

	/full_path_to_directory_of_dataset_files	
	HOLD:/full_path_to_directory_of_dataset_files	
	DONE:/full_path_to_directory_of_dataset_files	

Only the lines that do not begin with "HOLD:" or "DONE:" are processed.  This provides
an easy way to conduct piecemeal processing for very many datasets, allowing the file
to list datasets that are not yet ready for mapfile generation, as well as to maintain
a record of mapfiles already processed.

The lines to be processed are passed, one at a time, to the core script

	make_mapfile.sh

which serves to call "esgmapfile -make -i path_to_ini_files -outdir . . ."  hiding the
many details that tend to remain (near) constant.

This is merely a stopgap measure towards a fuller, data-driven automation and state
maintenance for mapfile generation.

