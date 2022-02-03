# The ESGF Publisher

The ESGF Publisher (`esgfpub`) library consists of scripts to automate publication of E3SM model outputs to ESGF. `esgfpub` has three main commands: [stage](#stage), [publish](#publish), and [check](#check).

- **_Note: The `esgfpub` module is not being actively developed and is decoupled from the `warehouse` module._**

## Prerequisites

1. Install Miniconda (recommended) or Anaconda

   - [https://docs.conda.io/en/latest/miniconda.html](https://docs.conda.io/en/latest/miniconda.html)

2. Clone the repo or fork

   ```bash
   git clone https://github.com/E3SM-Project/esgfpub.git
   ```

## Environment Setup By Use Case

### Development Environment

This environment is for testing local source code changes to the `esgfpub` modules before merging them to production (the `master` branch).

1. Open a branch from `master` for local development
2. Create and activate the environment

   ```bash
   cd esgfpub
    # Optionally, you can specify -n <NAME_OF_ENV> for a custom env name.
    # The default name is listed in dev.yml (`warehouse_dev`).
   conda env create -f conda-env/dev.yml
   conda activate warehouse_dev
   ```

3. Make changes to the source code in `/esgfpub`
4. Install local package with changes from source

   ```bash
      cd esgfpub
      pip install .
   ```

5. Test changes by following [ESGF Publication Usage](#esgf-publication-usage)
6. Add and commit changes, then push to remote branch
7. Open a pull request (PR) with this branch for review
8. Merge PR to `master`

## ESGF Publication Usage

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

### Stage

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

### Publish

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

### Check

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

## Warehouse Usage

There are two main modes of operation, either the full automated warehouse, or running any of the subordinate workflows individually. The automated warehouse can be run by itself with:

```bash
warehouse auto
```

The default behavior tries to make guesses for the correct mode of operation, but options can be changed manually as well

```bash
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
```

By default, the warehouse will collect ALL datasets from both the CMIP6 and E3SM project, and shepherd them towards publication, however the `--dataset-id` flag can be used to narrow the focus down to a specific dataset (by supplying the complete dataset_id), or to a subset of datasets (by supplying a substring of the dataset_id).

Example full dataset_id: `CMIP6.CMIP.E3SM-Project.E3SM-1-1.piControl.r1i1p1f1.Amon.cl.gr`
Example substring: `CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl` this will target all datasets from CMIP6 for the 1.0 version of the model, and the piControl experiment.

### Standalone workflow execution

Each of the subordinate workflows can be executed on a single dataset by themselves by using the following command (the example is Validation, but the same command will work for all the workflows):

```bash
warehouse validate --dataset-id <YOUR_DATASET_ID> --data-path <PATH_TO_YOUR_DATA>
```

The path should be to the directory one level up from the netCDF files themselves, and the files should be stored in a version directory, e.g. v0, v0.1, v1 etc

The full list of available workflows can be found in the top level help

```bash
>>> warehouse --help
usage: warehouse [-h] {publish,validate,auto,report,extract,cleanup,postprocess} ...

Automated E3SM data warehouse utilities

optional arguments:
  -h, --help            show this help message and exit

subcommands:
  warehouse subcommands

  {publish,validate,auto,report,extract,cleanup,postprocess}
    auto                Automated warehouse processing
    report              Print out a report of the dataset status for all datasets under the given root
```

```bash
>>> warehouse validate --help
usage: warehouse validate [-h] [--job-workers JOB_WORKERS] [-d [DATASET_ID ...]] [--data-path DATA_PATH]

Runs the Validation workflow on a single dataset. The input directory should be one level up from the data directory which should
me named v0, the input path will be used to hold the .status file and intermediate working directories for the workflow steps. The
--dataset-id flag should be in the facet format of the ESGF project. For CMIP6:
CMIP6.ScenarioMIP.CCCma.CanESM5.ssp126.r12i1p2f1.Amon.wap.gn For E3SM:
E3SM.1_0.historical.1deg_atm_60-30km_ocean.atmos.180x360.climo.164yr.ens5

optional arguments:
  -h, --help            show this help message and exit
  --job-workers JOB_WORKERS
                        number of parallel workers each job should create when running, default is 8
  -d [DATASET_ID ...], --dataset-id [DATASET_ID ...]
                        Dataset IDs that should have the workflow applied to them. If this is given without the data-path, the
                        default warehouse value will be used.
  --data-path DATA_PATH
                        Path to a directory containing a single dataset that should have the workflow applied to them. If given,
                        also use the --dataset-id flag to specify the dataset-id that should be applied to the data If its an E3SM
                        dataset, the ID should be in the form
                        'E3SM.model_version.experiment.(atm_res)_atm_(ocn_res)_ocean.realm.grid.data-type.freq.ensemble_number' for
                        example: 'E3SM.1_3.G-IAF-DIB-ISMF-3dGM.1deg_atm_60-30km_ocean.ocean.native.model-output.mon.ens1' If its a
                        CMIP6 dataset, the ID should be in the format 'CMIP6.activity.source.model-
                        version.case.variant.table.variable.gr' for example: 'CMIP6.CMIP.E3SM-
                        Project.E3SM-1-1.historical.r1i1p1f1.CFmon.cllcalipso.gr
```

## Warehouse Developer Guide

### Manipulating the job flow

Adding new jobs to a workflow, or manipulating the flow between jobs can be done entirely by changing values in the Transition Graphs for each workflow. Here's an example minimal set of transitions:

```bash
MPASVALIDATION:Ready:
  ocean-native-mon:
    -  MPASTimeCheck:Ready
  sea-ice-native-mon:
    -  MPASTimeCheck:Ready

MPASTimeCheck:Ready:
  default:
    -  MPASTimeCheck:Engaged

MPASTimeCheck:Pass:
  default:
    -  Pass

MPASTimeCheck:Fail:
  default:
    -  Fail
```

The "Pass"/"Fail" keywords are used to denote global success/failure for the whole workflow, otherwise each node in the graph should have exactly four entries, STEP:Ready, which transitions directly to STEP:Engaged:

```bash
MPASTimeCheck:Ready:
  default:
    -  MPASTimeCheck:Engaged
```

And then a STEP:Pass and a STEP:Fail

Each step has the ability to route different data-types to different subsequent steps, but every step should also include a "default" for routing any datasets that don't match any other explicit routing option.

### Adding workflow jobs

The names of each step need to match up with the NAME field (and the class name) for a WorkflowJob class in the /esgfpub/warehouse/workflows/jobs directory. The contents of this directory are dynamically loaded in at runtime, so if a new job is added, no additional imports are required.

Here's an example implementation of a WorkflowJob:

```bash
from warehouse.workflows.jobs import WorkflowJob

NAME  =  'CheckFileIntegrity'

class CheckFileIntegrity(WorkflowJob):
  def __init__(self, *args, **kwargs):
      super().__init__(*args, **kwargs)
      self.name = NAME
      self._requires = { '*-*-*': None }
      self._cmd = f"""
cd {self.scripts_path}
python check_file_integrity.py -p {self._job_workers}  {self.dataset.latest_warehouse_dir}
"""
```

- The class `name` field should match the NAME constant and the name of the class itself.
- The `_requires` field should be in the form of `realm-grid-freq` for example, `atmos-native-mon`, with `*` wildcards to denote "any." This sets up the required datasets needed for the job to execute, multiple entries are allowed.
- The `_cmd` field is where most of the work takes place, this is the string that will be placed in the slurm batch script, and the exit code of this string when executed will determine if the job goes to the Pass or Fail path in the parent workflow.

### Adding a new simulation

When a new simulation is slated for publication, the first thing that needs to happen is for it to be added to the dataset_spec.yaml under the warehouse/resources directory.

The dataset spec has two top level items, the Tables dictionary, which lists all the CMIP6 tables and the variables that are slated for publication in them, and the Projects dictionary, which contains all the information about the simulations we have published to the CMIP6 and E3SM project.

Dataset Spec structure:

```yaml
Project:
  CMIP6:
    Activity:
      model-version:
        experiment-name:
          start: first year of data
          end: last year of data
          ens: a list of variant labels
          except: a list of variables that arent included, all variables are assumed to be included unless they're in this list
  E3SM:
    model-version:
      experiment-name:
        start: first year of data
        end: last year of data
        ens: list of ensemble names
        except: list of variables not included
        campaign: campaign this exp is a member of
        science_driver: this exp science driver
        cmip_case: the CMIP name that this exp is published under, if applicable
        resolution:
          res-name:
            component:
              - grid: grid name
                data_types: list of  data-types with time freq
```

Here's an example of what that looks like in practice

```yaml
project:
  CMIP6:
    C4MIP:
      E3SM-1-1:
        hist-bgc:
          start: 1850
          end: 2014
          ens:
            - r1i1p1f1
          except:
            - siu
            - siv
            - clcalipso
  E3SM:
    "1_0":
      piControl:
        start: 1
        end: 500
        ens:
          - ens1
        except:
          - PRECSCS
          - PRECSCL
        campaign: DECK-v1
        science_driver: Water Cycle
        cmip_case: CMIP6.CMIP.E3SM-Project.E3SM-1-0.piControl
        resolution:
          1deg_atm_60-30km_ocean:
            land:
              - grid: native
                data_types:
                  - model-output.mon
              - grid: 180x360
                data_types:
                  - time-series.mon
            river:
              - grid: native
                data_types:
                  - model-output.mon
            atmos:
              - grid: 180x360
                data_types:
                  - climo.mon
                  - time-series.mon
              - grid: native
                data_types:
                  - model-output.day
                  - model-output.mon
                  - model-output.day_cosp
                  - model-output.6hr_snap
                  - model-output.3hr
                  - model-output.6hr
            ocean:
              - grid: native
                data_types:
                  - model-output.mon
                  - model-output.5day_snap
            sea-ice:
              - grid: native
                data_types:
                  - model-output.mon
            misc:
              - grid: native
                data_types:
                  - mapping.fixed
```

Once the new simulation has been added to the spec, the raw data needs to be staged before the datasets can be generated. Once the data is staged in the warehouse (or archive, once the extraction workflow has been implemented), you can run the `warehouse auto` to have the automation manage the processing and publication, or use the workflows directly to handle specific steps.

For example, if the above piControl experiment had just been added to the spec and included in the warehouse directory, you could run `warehouse auto --dataset-id E3SM.1_0.piControl*` to have it manage the workflows for all the datasets.
