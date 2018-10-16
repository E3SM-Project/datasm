"""
A user facing script for using the ESGF publication automation scripts
"""
#!/usr/bin/env python
from __future__ import print_function
import sys
import argparse
from configobj import ConfigObj
from utils import structure_gen, mapfile_gen, transfer_files

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument("config", help="Path to configuration file")
    ARGS = PARSER.parse_args()

    if not ARGS.config:
        PARSER.print_help()
        sys.exit(1)

    try:
        CONFIG = ConfigObj(ARGS.config)
    except SyntaxError as error:
        print("Unable to parse config file")
        print(repr(error))
        sys.exit(1)

    try:
        BASEOUTPUT = CONFIG['output_path']
        CASE = CONFIG['case']
        GRIDS = CONFIG['grids']
        ATMRES = CONFIG['atmospheric_resolution']
        OCNRES = CONFIG['ocean_resolution']
        FILE_TYPES = CONFIG['file_types']
    except ValueError as error:
        print('Unable to find values in config file')
        print(repr(error))
        sys.exit(1)

    try:
        structure_gen(
            basepath=BASEOUTPUT,
            casename=CASE,
            grids=GRIDS,
            atmos_res=ATMRES,
            ocean_res=OCNRES,
            file_types=FILE_TYPES)
    except IOError as error:
        print('Error generating file structure')
        print(repr(error))
        sys.exit(1)

    transfer_files(
        basepath=BASEOUTPUT,
        mode=CONFIG.get('transfer_mode', 'copy'),
        case=CASE,
        file_types=FILE_TYPES,
        excludes=CONFIG.get('excludes', ''))

    RUNMAPS = CONFIG['mapfiles']
    if RUNMAPS not in [True, 'true', 1, '1']:
        print('Not running mapfile generation')
        sys.exit(0)

    INIPATH = CONFIG['ini_path']
    NUMWORKERS = CONFIG['num_workers']
    mapfile_gen(
        basepath=BASEOUTPUT,
        inipath=INIPATH,
        casename=CASE,
        maxprocesses=NUMWORKERS)
