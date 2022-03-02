# The E3SM Data State Machine Warehouse

The E3SM Data State Machine (`datasm`) library automates complex nested workflows for handling E3SM outputs. These workflows use conditional branching based on the success or failure of the jobs within the workflows. The jobs include `extract`, `validate`, `postprocess`, and `publish`.

- **_Note: The `warehouse` is currently in active development, so many planned features may be a work in progress, missing, or broken._**

## Prerequisites

1. Install Miniconda (recommended) or Anaconda

   - [https://docs.conda.io/en/latest/miniconda.html](https://docs.conda.io/en/latest/miniconda.html)

2. Clone the repo or fork

   ```bash
   git clone https://github.com/E3SM-Project/esgfpub.git
   ```

Additional:

- Linux OS if you intend on building the [`warehouse` publishing environment](#publishing-environment) and running the warehouse `publish` operation

## Environment Setup By Use Case

### Development Environment

This environment is for testing local source code changes to the `warehouse` modules related to the
`extract`, `validate`, and `postprocess` operations before merging them to production (the `master` branch).

1. Open a branch from `master` for local development
2. Create and activate the environment

   ```bash
   # Optionally, you can specify -n <NAME_OF_ENV> for a custom env name.
   # The default name is listed in dev.yml (`warehouse_dev`).
   conda env create -f conda-env/dev.yml
   conda activate warehouse_dev
   ```

3. (Optional) Make changes to the `extract`, `validate`, and `postprocess` modules in `/warehouse`
4. Install package from local build

   ```bash
      cd datasm
      pip install .
   ```

5. Test changes by following [Warehouse Usage](#warehouse-usage)
6. Add and commit changes, then push to remote branch
7. Open a pull request (PR) with this branch for review
8. Merge PR to `master`

### Production Environment

This environment is used for performing warehouse production operations (`extract`, `validate`, `postprocess`) using the latest stable releases of the dependencies listed in the env's yml file (`prod.yml`).

1. Create and activate the environment

   ```bash
   # Optionally, you can specify -n <NAME_OF_ENV> for a custom env name.
   # The default name is listed in prod.yml (`warehouse_prod`).
   conda env create -f conda-env/prod.yml
   conda activate warehouse_prod
   ```

2. Proceed to [Warehouse Usage](#warehouse-usage) section

### Publishing Environment

This environment is for testing local source code changes to the `warehouse` modules related to the `publish` operation before merging to `master`. It includes the latest stable releases of `esgf-forge` dependencies.

- `autocurator=0.1` is only available for Linux and does not support `python>3.8`
- `autocurator=0.1` requires `libnetcdf >=4.7.4,<4.7.5.0a0`, which is not compatible with `nco>=5`
- As a result of these conflicts, `esgf-forge` dependencies could not be included in `prod.yml`

1. Open a branch from `master` for local development
2. Create and activate the environment

   ```bash
    # Optionally, you can specify -n <NAME_OF_ENV> for a custom env name.
    # The default name is listed in pub.yml (`warehouse_pub`).
   conda env create -f conda-env/pub.yml
   conda activate warehouse_pub
   ```

3. (Optional) Make changes to the `publish` modules in `/warehouse`
4. Install local package from local build

   ```bash
      cd warehouse
      pip install .
   ```

5. Test changes by following [Warehouse Usage](#warehouse-usage)
6. Add and commit changes, then push to remote branch
7. Open a pull request (PR) with this branch for review
8. Merge PR to `master`

#### How to Update Dependencies

Occasionally, the dependencies in the conda env yml files are updated to include the latest bug fixes or features. To update the dependencies in the yml files remotely and pull them in locally to your environments:

1. Check the latest version on Anaconda's package repository [website](https://anaconda.org/).
2. Open a branch from the latest `master`
3. Update pinned version(s) of dependencies in yml files
4. Open PR for review, then merge to `master` when ready
5. Checkout `master` branch locally and pull latest changes

   ```bash
   git checkout master && git pull
   ```

6. Update existing conda envs with yml files:

   ```bash
   # -n <NAME_OF_ENV> must be specified if you used a custom env name instead of the default name found in the yml file.
   conda env update --f conda-env/<NAME_OF_YML>.yml
   ```

## Warehouse Usage

There are two main modes of operation, either the full automated warehouse, or running any of the subordinate workflows individually. The automated warehouse can be run by itself with:

```bash
datasm auto
```

The default behavior tries to make guesses for the correct mode of operation, but options can be changed manually as well

```bash
usage: datasm auto [-h] [-n NUM] [-s] [-w WAREHOUSE_PATH] [-p PUBLICATION_PATH] [-a ARCHIVE_PATH] [-d DATASET_SPEC]
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
                        default=/datasm/resources/dataset_spec.yaml
  --dataset-id [DATASET_ID ...]
                        Only run the automated processing for the given datasets, this can the the complete dataset_id, or a
                        wildcard such as E3SM.1_0.
  --job-workers JOB_WORKERS
                        number of parallel workers each job should create when running, default=8
  --testing             run the datasm in testing mode
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
datasm validate --dataset-id <YOUR_DATASET_ID> --data-path <PATH_TO_YOUR_DATA>
```

The path should be to the directory one level up from the netCDF files themselves, and the files should be stored in a version directory, e.g. v0, v0.1, v1 etc

The full list of available workflows can be found in the top level help

```bash
>>> datasm --help
usage: datasm [-h] {publish,validate,auto,report,extract,cleanup,postprocess} ...

Automated E3SM Data State Machine utilities

optional arguments:
  -h, --help            show this help message and exit

subcommands:
  datasm subcommands

  {publish,validate,auto,report,extract,cleanup,postprocess}
    auto                Automated processing
    report              Print out a report of the dataset status for all datasets under the given root
```

```bash
>>> datasm validate --help
usage: datasm validate [-h] [--job-workers JOB_WORKERS] [-d [DATASET_ID ...]] [--data-path DATA_PATH]

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
                        default datasm value will be used.
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
from datasm.workflows.jobs import WorkflowJob

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

When a new simulation is slated for publication, the first thing that needs to happen is for it to be added to the dataset_spec.yaml under the `warehouse/resources` directory.

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
