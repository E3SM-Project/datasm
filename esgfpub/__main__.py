"""
A tool for automating much of the ESGF publication process
"""

from esgfpub.util import transfer_files, mapfile_gen
import argparse
import sys
from threading import Event
from esgfpub.util import print_message
from configobj import ConfigObj


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument(
        "config", 
        help="Path to configuration file")
    PARSER.add_argument(
        '--over-write', 
        help="Over write any existing files", 
        action='store_true')
    ARGS = PARSER.parse_args()

    if not ARGS.config:
        PARSER.print_help()
        sys.exit(1)
    
    if ARGS.over_write:
        overwrite = True
    else:
        overwrite = False

    try:
        CONFIG = ConfigObj(ARGS.config)
    except SyntaxError as error:
        print_message("Unable to parse config file")
        print(repr(error))
        sys.exit(1)

    try:
        BASEOUTPUT = CONFIG['output_path']
        GRID = CONFIG['non_native_grid']
        ATMRES = CONFIG['atmospheric_resolution']
        OCNRES = CONFIG['ocean_resolution']
        DATA_PATHS = CONFIG['data_paths']
        ENSEMBLE = CONFIG['ensemble']
        EXPERIMENT_NAME = CONFIG['experiment']
    except ValueError as error:
        print_message('Unable to find values in config file')
        print(repr(error))
        sys.exit(1)

    print_message('Transfering files', 'ok')
    ret = transfer_files(
        outpath=BASEOUTPUT,
        experiment=EXPERIMENT_NAME,
        grid=GRID,
        mode=CONFIG.get('transfer_mode', 'copy'),
        data_paths=DATA_PATHS,
        ensemble=ENSEMBLE,
        overwrite=overwrite)
    if ret == -1:
        sys.exit(1)

    RUNMAPS = CONFIG.get('mapfiles', False)
    if not RUNMAPS or RUNMAPS not in [True, 'true', 'True', 1, '1']:
        print_message('Not running mapfile generation', 'ok')
        print_message('Publication prep complete', 'ok')
        sys.exit(0)

    INIPATH = CONFIG['ini_path']
    NUMWORKERS = CONFIG['num_workers']
    event = Event()

    try:
        print_message('Starting mapfile generation', 'ok')
        res = mapfile_gen(
            basepath=BASEOUTPUT,
            inipath=INIPATH,
            experiment=EXPERIMENT_NAME,
            maxprocesses=NUMWORKERS,
            event=event)
    except KeyboardInterrupt as error:
        print_message('Keyboard interrupt ... exiting')
        event.set()
    else:
        if res == 0:
            print_message('Publication prep complete', 'ok')
