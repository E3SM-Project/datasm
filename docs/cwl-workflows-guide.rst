.. _cwl workflows:

*************
CWL Workflows
*************

There is a set of CWL workflow scripts in the repository (``/scripts/cwl_workflows``) for each realm. Each workflow breaks the input files up into manageable segment size and perform all the required input processing needed before invoking ``e3sm_to_cmip``. These scripts have been designed to run on a SLURM cluster in parallel and will process an arbitrarily large set of simulation data in whatever chunk size required.


Setting up your CWL environment
###############################

To use the CWL workflows you will need additional dependencies in your environment:

.. code-block:: text

    conda install -c conda-forge cwltool nodejs

When CWL runs it needs somewhere to store its intermediate files. By default it will use the systems $TMPDIR
but in some cases that wont work, for example on NERSC the compute nodes wont have access to the login nodes /tmp directory.
An easy solution for this is to create a directory on a shared mount, and run ``export TMPDIR=/path/to/shared/location`` and
then when running the cwltool use the ``--tmpdir-prefix=$TMPDIR`` argument.

Using the CWL Workflows
#######################

Each of the directories under ``scripts/cwl_workflows`` holds a single self-contained workflow.
The name of the workflow matches the name of the directory, for example under the mpaso directory is a file named ``mpaso.cwl`` which contains the workflow.

The beginning of each workflow contains an ``inputs`` section which defines the required parameters, for example

.. code-block:: yaml

    inputs:
        data_path: string
        metadata: File
        workflow_output: string

        mapfile: File
        frequency: int

        namelist_path: string
        region_path: string
        restart_path: string

        tables_path: string
        cmor_var_list: string[]

        timeout: int
        partition: string
        account: string

Along with each of the cwl workflows is an example yaml parameter file, for example along with ``mpaso.cwl`` is 
``mpaso-job.yaml`` which contains the following:

.. code-block:: yaml

    data_path: /p/user_pub/e3sm/staging/prepub/1_1_ECA/ssp585-BDRD//1deg_atm_60-30km_ocean/ocean/native/model-output/mon/ens1/v0/
    workflow_output: /p/user_pub/e3sm/baldwin32/workshop/ssp585/ssp585/output/pp/cmor/ssp585/2015_2100
    
    metadata:
        class: File
        path: /p/user_pub/e3sm/baldwin32/workshop/ssp585/ssp585/output/pp/cmor/ssp585/2015_2100/user_metadata.json
    mapfile:
        class: File
        path: /export/zender1/data/maps/map_oEC60to30v3_to_cmip6_180x360_aave.20181001.nc

    frequency: 5
    namelist_path: /p/user_pub/e3sm/baldwin32/workshop/E3SM-1-1-ECA.hist-bgc/mpaso_in
    region_path: /p/user_pub/e3sm/baldwin32/resources/oEC60to30v3_Atlantic_region_and_southern_transect.nc
    restart_path: /p/user_pub/e3sm/baldwin32/workshop/E3SM-1-1-ECA.hist-bgc/mpaso.rst.1851-01-01_00000.nc
    tables_path: /export/baldwin32/projects/cmor/Tables

    timeout: 10:00:00
    account: e3sm
    partition: debug

    cmor_var_list: [masso, volo, thetaoga, tosga, soga, sosga, zos, masscello, tos, tob, sos, sob, mlotst, fsitherm, wfo, sfdsi, hfds, tauuo, tauvo, thetao, so, uo, vo, wo, hfsifrazil, zhalfo]

Once the parameter file is complete, the workflow can be executed by calling the cwltool

.. code-block:: text

    cwltool --tmpdir-prefix=$TMPDIR ~/projects/e3sm_to_cmip/scripts/cwl_workflows/mpaso/mpaso.cwl mpaso-job.yaml


End-to-End High Frequency Example
=================================

The first step is to check what variables in the raw input data are possible to be converted at the desired frequency. For this we need to use the "info" option and give it three
things, the frequency of data we want to convert, the input path to the raw data (not time-series, but native model output), and the location of our copy of the CMIP6 controlled vocabulary tables:

.. code-block:: bash

    >> e3sm_to_cmip --info -v all --input /p/user_pub/work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/atmos/native/model-output/day/ens1/v1/ --tables ~/projects/cmip6-cmor-tables/Tables/
    [*]
    CMIP6 Name: huss,
    CMIP6 Table: CMIP6_day.json,
    CMIP6 Units: 1,
    E3SM Variables: QREFHT
    [*]
    CMIP6 Name: tas,
    CMIP6 Table: CMIP6_day.json,
    CMIP6 Units: K,
    E3SM Variables: TREFHT
    [*]
    CMIP6 Name: tasmin,
    CMIP6 Table: CMIP6_day.json,
    CMIP6 Units: K,
    E3SM Variables: TREFHTMN
    [*]
    CMIP6 Name: tasmax,
    CMIP6 Table: CMIP6_day.json,
    CMIP6 Units: K,
    E3SM Variables: TREFHTMX
    [*]
    CMIP6 Name: rlut,
    CMIP6 Table: CMIP6_day.json,
    CMIP6 Units: W m-2,
    E3SM Variables: FLUT


The next step is to find and setup the corresponding CWL workflow, in this case since we're processing daily data we want to use the "atm-day" workflow under
e3sm_to_cmip/scripts/cwl_workflows `which you can find here <https://github.com/E3SM-Project/e3sm_to_cmip/tree/master/scripts/cwl_workflows/atm-day>`_. The CWL parameter
file atm-day-job.yaml needs to be edited with the values for our case. We need to take the E3SM variable names given by the "--info" request earler and put them into the
``std_var_list`` parameter, and take the CMIP6 variable names and put them into the ``std_cmor_list`` parameter. Create a new directory to hold your output, and place
the new parameter file there.

.. code-block:: yaml

    # path to the raw model data
    data_path: /p/user_pub/work/E3SM/1_0/historical/1deg_atm_60-30km_ocean/atmos/native/model-output/day/ens1/v1/

    # size of output data files in years
    frequency: 25

    # number of ncremap workers
    num_workers: 12

    # slurm account info
    account: e3sm
    partition: debug
    timeout: 2:00:00

    # horizontal regridding file path
    hrz_atm_map_path: /export/zender1/data/maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc

    # path to CMIP6 tables directory
    tables_path: /export/baldwin32/projects/cmip6-cmor-tables/Tables/

    # path to CMOR case metadata
    metadata_path: /p/user_pub/e3sm/baldwin32/resources/CMIP6-Metadata/1.0/historical_ens1.json

    # list if E3SM raw variable names
    std_var_list: [QREFHT, TREFHT, TREFHTMN, TREFHTMX, FLUT]

    # list of CMIP6 variable names
    std_cmor_list: [huss, tas, tasmin, tasmax, rlut]

Make a temp directory to contain the intermediate files created by the workflow, and set it as your TMPDIR

.. code-block:: bash

    cd /p/user_pub/e3sm/baldwin32/workshop/highfreq/1.0/historical
    mkdir tmp
    export TMPDIR=/p/user_pub/e3sm/baldwin32/workshop/highfreq/1.0/historical/tmp

And startup the CWL workflow

.. code-block:: bash

    >> cwltool --tmpdir-prefix=$TMPDIR --preserve-environment UDUNITS2_XML_PATH ~/projects/e3sm_to_cmip/scripts/cwl_workflows/atm-day/atm-day.cwl historical-atm-day-ens1.yaml

This will launch a fairly long running job as it steps through all the parts of the workflow. If you're running a very large set of data, it can help to use the ``nohup`` tool to
wrap the command so it doesnt get interupted by logging out.