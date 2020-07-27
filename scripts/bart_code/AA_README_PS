 Publication Status Page Preparation Report

The routine:

	pub_stat_page_prep.sh pub_dir_pathhead	(typically, /p/user_pub/work/E3SM/)

This will find the leaf dirs under the publication directories, and output lines of the form

	ModVer,Resolution,Ens,Title,Status,Grid,Years,Filecount,Dataset_ID,Dataset_First_File,Dataset_Publication_Path,Notes,Title_Expanded

Explicitly:

	ModVer:			A component of the path
	Resolution:		A component of the path
	Ens			A component of the path
	Title:			(RealmCode Grid Freq) derived from path elements.
	Status:			(placeholder for publication status)
	Grid:			A component of the path
	Years:			(placeholder for dataset years covered)
	Filecount:		From live directory examination
	Dataset_ID		Derived from path
	Dataset_First_File:	From live directory examination
	Dataset_Publication_Path:  Derived from path
	Notes:			(placeholder)
	Title_Expanded:		(placeholder)

For ALL model version experiments, ensembles, dataset_types and publication versions.

By converting this CSV output into an Excel document, the Excel rows can be copy/pasted directly into the associate confluence tables.

All that is missing by this operation is the ablity to determine the actual publication status for each dataset.

Formerly, this was obtained by calling "esglist_datasets" with the status flag, on aims3.

	Before running esglist_datasets:

		(activate the esgf-pub environment)
		source /usr/local/conda/bin/activate esgf-pub

		(if 72 hour auth cert has timed out.  have your esgf-node password ready)
		myproxy-logon -s esgf-node.llnl.gov -l <your_esgf_user_name> -t 72 -o ~/.globus/certificate-file

