# ESGF Publisher and E3SM Automated Warehouse

This repository contains two modules:
1. The ESGF Publisher, which are scripts to publish data to ESGF.
2. The E3SM Automated Warehouse, which is a utility that allows for the automation of complex nested workflows with conditional branching based on the success or failure of the jobs.

## Developer Guide

### Prerequisites
1. Linux OS (`esgf-forge/autocurator` Anaconda package only available on Linux)
2. Miniconda or Anaconda installed
3. Cloned the repo or fork
   ```bash
   git clone https://github.com/E3SM-Project/esgfpub.git
   cd esgfpub
   ```

### Getting Started

#### Development Environment

This environment is intended for local development of future releases. It does not include the latest stable release of `esgfpub`.  

1. Create the Anaconda development environment
    ```bash
    cd esgfpub
    conda env create -f conda/dev.yml
    conda activate esgfpub_dev
    ```
2. Make changes to files in `esgfpub/` or `warehouse/` 
3. Test changes by following these instructions
   - [`esgfpub/`](esgfpub/README.md#installation)
   - [`warehouse/`](warehouse/README.md#installation)
4. Commit and push 
