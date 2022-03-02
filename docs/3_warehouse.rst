The E3SM Automated Warehouse
============================

The warehouse utility allows for the automation of complex nested
workflows with conditional branching based on the success or failure of
the jobs.

*Note: The warehouse is currently in active development, so many planned
features may be missing or broken.*

Installation
------------

1. Set up development environment `here`_

2. Install local package with changes

   .. code:: bash

         cd esgfpub/warehouse
         python setup.py install
         python setup.py clean

3. Get the custom E3SM branch of the esgf publisher utility

   .. code:: bash

      git clone https://github.com/sashakames/esg-publisher -b e3sm-custom
      cd esg-publisher/pkg
      python setup.py install

Usage
-----

There are two main modes of operation, either the full automated
warehouse, or running any of the subordinate workflows individually. The
automated warehouse can be run by itself with:

::

   warehouse auto

The default behavior tries to make guesses for the correct mode of
operation, but options can be changed manually as well

::

   usage: warehouse auto [-h] [-n NUM] [-s] [-w WAREHOUSE_PATH] [-p PUBLICATION_PATH] [-a ARCHIVE_PATH] [-d DATASET_SPEC]
                         [--dataset-id [DATASET_ID ...]] [--job-workers JOB_WORKERS] [--testing] [--sproket SPROKET]
                         [--slurm-path SLURM_PATH] [--report-missing]

   optional arguments:
     -h, --help            show this help message and exit
     -n NUM, --num NUM     Number of parallel workers
     -s, --serial          Run everything in serial
     -w WAREHOUSE_PATH, --warehouse-path WAREHOUSE_PATH
                           The root path for pre-publication dataset staging, default=/p/user_pub/e3sm/warehouse/
     -p PUBLICATION_PATH, --publication-path PUBLICATION_PATH
                           The root path for data publication, default=/p/user_pub/work/
     -a ARCHIVE_PATH, --archive-path ARCHIVE_PATH
                           The root path for the data archive, default=/p/user_pub/e3sm/archive
     -d DATASET_SPEC, --dataset-spec DATASET_SPEC
                           The path to the dataset specification yaml file,
                           default=/warehouse/resources/dataset_spec.yaml
     --dataset-id [DATASET_ID ...]
                           Only run the automated processing for the given datasets, this can the the complete dataset_id, or a
                           wildcard such as E3SM.1_0.
     --job-workers JOB_WORKERS
                           number of parallel workers each job should create when running, default=8
     --testing             run the warehouse in testing mode
     --sproket SPROKET     path to sproket binary if its not in your $PATH
     --slurm-path SLURM_PATH
                           The directory to hold slurm batch scripts as well as console output from batch jobs,
                           default=$PWD/slurm_scripts
     --report-missing      After collecting the datasets, print out any that have missing files and exit

By default, the warehouse will collect ALL datasets f

.. _here: 1_developer_guide.rst#Getting-Started