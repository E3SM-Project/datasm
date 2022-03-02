Automated publication to ESGF for E3SM model output
===================================================

The esgfpub tool consists of three main commands: `stage`_, `publish`_
and `check`_.

Installation
------------

Development Environment
~~~~~~~~~~~~~~~~~~~~~~~

1. Set up development environment `here`_

2. Install local package with changes from source

   .. code:: bash

      cd esgfpub/esgfpub
      python setup.py install
      python setup.py clean

Latest Stable Release
~~~~~~~~~~~~~~~~~~~~~

1. Install from Anaconda

   .. code:: bash

      conda install -c e3sm esgfpub

Usage
-----

.. code:: bash

   >>> esgfpub -h
   usage: esgfpub [-h] {stage,check,publish} ...

   optional arguments:
     -h, --help            show this help message and exit

   subcommands:
     valid subcommands

     {stage,check,publish}
       stage               Move data and generate mapfiles
       check               Check the file structure and ESGF database for missing
                           datasets
       publish             Publish a directory of mapfiles to ESGF

Stage
~~~~~

The "stage" subcommand is used to move data from holding directories
into the correct ESGF directory structure given the facets of the case.
This tool is expected to be used on a per-case basis, and currently only
supports staging data from a single case at once.

.. code:: bash

   >>> esgfpub stage -h
   usage: esgfpub stage [-h] [-t TRANSFER_MODE] [--over-write] [-o MAPOUT]
                        [--debug]
                        config

   positional arguments:
     config                Path to configuration file

   optional arguments:
     -h, --help            show this help message and exit
     -t TRANSFER_MODE, --transfer-mode TRANSFER_MODE
                           the file transfer mode, allowed values are link, move,
                           or copy
     --over-write          Over write any existing files
     -o MAPOUT, --output-mapfiles MAPOUT
                           The output location for mapfiles, defaults to
                           ./mapfiles/
     --debug

The main requirement for the stage command is a configuration file in
the yaml format listing out the ESGF search facets for the case (used to
generate the directory structure), and a listing of the source
directories to pull the model data from. Here's an example config:

.. code:: yaml

   output_path: /p/user_pub/work/  <- The base publication directory
   project: E3SM                   <- The name of the project under ESGF
   experiment: piControl           <- The name of the case
   ensemble: ens1                  <- The name of this ensemble member
   non_native_grid: 180x360        <- The name of the atmos/land the data was regridded to
   atmospheric_resolution: 1deg    <- The resolution of the atmos component when the model was run
   ocean_resolution: 60-30km       <- The resolution of the MPAS component when the model was run

   start_year: 1850                <- The start year of the data, used to verify all files are in place
   end_year: 2014                  <- The last year of data

   mapfiles: true                 <- Controls if ESGF mapfiles are generate

.. _stage: #Stage
.. _publish: #Publish
.. _check: #Check
.. _here: ../README.rst#Getting-Started