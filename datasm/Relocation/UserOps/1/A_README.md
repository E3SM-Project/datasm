This directory is intended to be the "current directory" when performing
data acqusition and acceptance operations.  These include:

    Globus transfer scripts and logs

    Definition of new archive locations and naming (and ensemble identification)

        The experiment ensemble numbers, (ens1, ens2, ens3, ...) are often not
        obvious from the archive names.  In CMIP6 generation they will correspond
        to the realization index values (r1,r2,r3, ...) of the variant labels.
        Ensure you have confirmation of the archive ensemble numbers BEFORE proceeding
        with subsequent operations, to avoid considerable wasted effort and the need
        to make extensive remediations.

        Examine sample "namefile" to ensure the mapping between h-codes and
        frequencies is understood.  Update the Standard_Datatype_Extraction_Patterns file
        ([ARCHIVE_MANAGEMENT]/Standard_Datatype_Extraction_Patterns) if needed.
        Some campaigns employ these codes differently.

    Update and maintenance of the dataset_spec.yaml (both E3SM and External)
        E3SM:  [STAGING_RESOURCE]/dataset_spec.yaml
        Other: [STAGING_RESOURCE]/External/*

    Generation and maintenance of the associated metadata files.
        Use subdirectory "metadata_management".
        See:   [STAGING_RESOURCE]/CMIP6-Metadata (and CMIP6-Metadata-Repo)


