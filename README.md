# The ESGF Publisher and E3SM Automated Warehouse

This repository contains two sub-repos:

1. The ESGF Publisher (`esgfpub`) library consists of scripts to automate publication of E3SM model outputs to ESGF. `esgfpub` has three main commands: [stage](#stage), [publish](#publish), and [check](#check).

   - **_Note: The `esgfpub` module is not being actively developed and is decoupled from the `warehouse` module._**

2. The E3SM Automated Warehouse (`warehouse`) library automates complex nested workflows for handling E3SM outputs. These workflows use conditional branching based on the success or failure of the jobs within the workflows. The jobs include `extract`, `validate`, `postprocess`, and `publish`.

   - **_Note: The warehouse is currently in active development, so many planned features may be a work in progress, missing, or broken._**

Visit the [docs](docs/) for more information on how to get started.
