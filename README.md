# Automated publication to ESGF for E3SM model output

Moves model output from staging directories into the ESGF publication structure, and then optionally creates the ESGF mapfiles for publication.

```bash

usage: esgfpub [-h] [-t TRANSFER_MODE] [--over-write] config
positional arguments:
  config                Path to configuration file
optional arguments:
  -h, --help            show this help message and exit
  -t TRANSFER_MODE, --transfer-mode TRANSFER_MODE
                        the file transfer mode, allowed values are link, move,
                        or copy
  --over-write          Over write any existing files

```