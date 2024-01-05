
Due to the huge processing load and durations required for DataSM PostProcessing, in particular for
CMIP6 generation with over a hundred single-variable datasets generated per ensemble, it is useful
to have an array of "Ops" directories, each with its own list of dataset_ids to produce, and its own
runtime logs and job configuation directories to review.  The also prevents the commonly-named
"slurm_scripts" runtime directories that are created from interfering when multiple Ops queues are
in play.

To this end, there are currently 9 Ops directories [Ops1 .. Ops9], and each contains the following:

        DS_Spec
        target-0

Briefly, one partitions some set of desired (e.g.) CMIP6 dataset_ids, perhaps a few thousand, into
groups according to whatever load-balancing scheme one feels is appropropriate.  Each such list of
dataset_ids should be given a unique name of the form:

        to_generate_<unique_description>_<number_of_dataset_ids>

    For example:

        to_generate_v2_NARRM_CMIP6_historical_330

NOTE: All datasets in a given "Ops" queue must employ the same defining "dataset_spec.yaml" file.
Edit the file "DS_SPEC" to ensure only the appropriate dataset_spec is un-commented:

    #/p/user_pub/e3sm/staging/resource/dataset_spec.yaml
    #/p/user_pub/e3sm/archive/External/E3SMv1_LE/resource/v1_LE_dataset_spec.yaml
    /p/user_pub/e3sm/archive/External/E3SMv2_LE/resource/v2_LE_dataset_spec.yaml

To allow the "progress_Ops.sh" script to track progress successfully, rename the (empty) file:

        target-0
    to
        target-330  (however many dataset_ids are in the local dsid_list)

To launch the jobs for this queue, issue:

        nohup ../run_with_spec.sh <the_dsid_list> &

    For example:

        nohup ../run_with_spec.sh to_generate_v2_NARRM_CMIP6_historical_320 &

Over time (which can be days or weeks) the Ops directory will change from

        DS_Spec
        target-330
        to_generate_v2_NARRM_historical_330

    into

        DS_Spec
        PostProcess-Log-<timestamp>-<dataset_id>
        PostProcess-Log-<timestamp>-<dataset_id>
        PostProcess-Log-<timestamp>-<dataset_id>
        ...
        slurm_scripts/
        slurm_scripts-<timestamp>-<dataset_id>
        slurm_scripts-<timestamp>-<dataset_id>
        slurm_scripts-<timestamp>-<dataset_id>
        ...
        target-330
        to_generate_v2_NARRM_CMIP6_historical_330

When generating thousands of CMIP6 datasets, one would partition the entire list of dataset_ids into
separate lists, either per experiment like this

        to_generate_v2_NARRM_CMIP6_1pctCO2_110
        to_generate_v2_NARRM_CMIP6_abrupt4xCO2_220      (2 ensembles of 110)
        to_generate_v2_NARRM_CMIP6_historical_550       (5 ensembles of 110)
        ...

or by groups of ensembles (recommended for load balancing):

        to_generate_v2_NARRM_CMIP6_ens_1-4_440
        to_generate_v2_NARRM_CMIP6_ens_5-8_440
        to_generate_v2_NARRM_CMIP6_ens_9-12_440
        ...
        
or even by Realm/Freq table, if one anticipates problems with some:

        to_generate_v2_NARRM_CMIP6_Amon_760
        to_generate_v2_NARRM_CMIP6_CFmon_84
        to_generate_v2_NARRM_CMIP6_Lmon_228
        ...


