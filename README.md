# Automated publication to ESGF for E3SM model output

The esgfpub tool consists of three main commands: [stage](#Stage), [publish](#Publish) and [check](#Check). 

```bash
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

```

## Stage

The "stage" subcommand is used to move data from holding directories into the correct ESGF directory structure given the facets of the case. This tool is expected to be used on a per-case basis, and currently only supports staging data from a single case at once. 

```bash
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
```



The main requirement for the stage command is a configuration file in the yaml format listing out the ESGF search facets for the case (used to generate the directory structure), and a listing of the source directories to pull the model data from. Here's an example config:

```yaml
output_path: /p/user_pub/work/  <- The base publication directory
project: E3SM                   <- The name of the project under ESGF
experiment: piControl           <- The name of the case
ensemble: ens1                  <- The name of this ensemble member
non_native_grid: 180x360        <- The name of the atmos/land the data was regridded to
atmospheric_resolution: 1deg    <- The resolution of the atmos component when the model was run
ocean_resolution: 60-30km       <- The resolution of the MPAS component when the model was run

start_year: 1850                <- The start year of the data, used to verify all files are in place
end_year: 2014                  <- The last year of data

mapfiles: true                 <- Controls if ESGF mapfiles are generated after moving the data
num_workers: 24                <- The number of parallel workers to use when hashing files
ini_path: /path/to/ini/directory <- Path to directory containing the ESGF ini files

data_paths:
    atmos: /path/to/atmos/data 
    land: /path/to/land/data
    sea-ice: /path/t/sea-ice/data
    ocean: /path/to/ocean/data
```

## Publish

The "publish" subcommand is used to publish a directory full of mapfiles.

```bash
>>> esgfpub publish -h
usage: esgfpub publish [-h] [--maps-in MAPS_IN] [--maps-done MAPS_DONE]
                       [--maps-err MAPS_ERR] [--ini INI] [--loop]
                       [--username USERNAME] [--debug]

optional arguments:
  -h, --help            show this help message and exit
  --maps-in MAPS_IN     Path to input mapfile directory
  --maps-done MAPS_DONE
                        Path to where complete mapfiles should be moved to
  --maps-err MAPS_ERR   Path to where errored mapfiles should be moved to
  --ini INI             Path to ini directory
  --loop                If set, this will cause the publisher to loop
                        continuously and publish any mapfiles placed in the
                        input directory
  --username USERNAME   Username for myproxy-logon
  --debug
```

## Check

The "check" subcommand is used to check the consistancy of published datasets. Its two modes are used to a) check that every file that should be present in the selected datasets is present, b) check that no extra files are included, and optionally c) run a simple squared deviance check on CMIP6 time-series data to detect inconsistancies in the data.

The publication checks use the [sproket](https://github.com/ESGF/sproket) tool to interact with the ESGF search API. Download the binary and put it somewhere in your $PATH, or specify its location with the `--sproket` command.

```bash
>>> esgfpub check -h
usage: esgfpub check [-h] [-p PROJECT] [-c CASES [CASES ...]]
                     [-v VARIABLES [VARIABLES ...]] [-t TABLES [TABLES ...]]
                     [--ens ENS [ENS ...]] [-d DATASET_IDS [DATASET_IDS ...]]
                     [--published] [-m MAX_CONNECTIONS] [--sproket SPROKET]
                     [--file-system] [--data-path DATA_PATH]
                     [--model-versions MODEL_VERSIONS [MODEL_VERSIONS ...]]
                     [--verify] [--case-spec CASE_SPEC] [--to-json TO_JSON]
                     [-s] [--debug]

optional arguments:
  -h, --help            show this help message and exit
  -p PROJECT, --project PROJECT
                        Which project to check for, valid arguments are cmip6
                        or e3sm. Default is both
  -c CASES [CASES ...], --cases CASES [CASES ...]
                        Which case to check the data for, default is all
  -v VARIABLES [VARIABLES ...], --variables VARIABLES [VARIABLES ...]
                        Which variables to check for, default is all
  -t TABLES [TABLES ...], --tables TABLES [TABLES ...]
                        List of CMIP6 tables or E3SM data-types to search for,
                        default is all
  --ens ENS [ENS ...], --ensembles ENS [ENS ...]
                        List of ensemble members to check, default all
  -d DATASET_IDS [DATASET_IDS ...], --dataset-ids DATASET_IDS [DATASET_IDS ...]
                        One or more dataset IDs to check, if this option is
                        turned on only these datasets will be checked
  --published           Check the LLNL ESGF node to see if the variables have
                        been published
  -m MAX_CONNECTIONS, --max-connections MAX_CONNECTIONS
                        Maximum number of simultanious connections to the ESGF
                        node, only needed if --published is turned on. default
                        = 5
  --sproket SPROKET     Path to custom sproket binary, only needed if
                        --published is turned on.
  --file-system         Check the data is present on the filesystem under the
                        --data-path directory
  --data-path DATA_PATH
                        path to the root directory containing the local data
  --model-versions MODEL_VERSIONS [MODEL_VERSIONS ...]
                        versions of the model to add to the search, default is
                        all
  --verify              Run a std deviation test on global mean for each
                        variable
  --case-spec CASE_SPEC
                        Path to custom dataset specification file
  --to-json TO_JSON     The output will be stored in the given file, json
                        format
  -s, --serial          Should this be run in serial, default is parallel.
  --debug
```