"""
A tool for automating much of the ESGF publication process
"""

from esgfpub.util import transfer_files, mapfile_gen, validate_raw, makedir
import argparse
import sys
import os
from threading import Event
from esgfpub.util import print_message
from configobj import ConfigObj
from tqdm import tqdm

def main():
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
        return 1
    
    if ARGS.over_write:
        overwrite = True
    else:
        overwrite = False

    try:
        CONFIG = ConfigObj(ARGS.config)
    except SyntaxError as error:
        print_message("Unable to parse config file")
        print(repr(error))
        return 1

    try:
        BASEOUTPUT = CONFIG['output_path']
        GRID = CONFIG.get('non_native_grid')
        ATMRES = CONFIG['atmospheric_resolution']
        OCNRES = CONFIG['ocean_resolution']
        DATA_PATHS = CONFIG['data_paths']
        ENSEMBLE = CONFIG['ensemble']
        EXPERIMENT_NAME = CONFIG['experiment']
        START = int(CONFIG['start_year'])
        END = int(CONFIG['end_year'])
    except ValueError as error:
        print_message('Unable to find values in config file')
        print(repr(error))
        return 1

    print_message('Validating raw data', 'ok')
    if not validate_raw(DATA_PATHS, START, END):
        return 1

    resdirname = "{}_atm_{}_ocean".format(ATMRES, OCNRES)
    makedir(os.path.join(BASEOUTPUT, EXPERIMENT_NAME, resdirname))

    if CONFIG.get('transfer_mode', 'copy') == 'move':
        print_message('Moving files', 'ok')
    elif CONFIG.get('transfer_mode', 'copy') == 'copy':
        print_message('Copying files', 'ok')
    elif CONFIG.get('transfer_mode', 'copy') == 'link':
        print_message('Linking files', 'ok')
    num_moved = transfer_files(
        outpath=BASEOUTPUT,
        experiment=EXPERIMENT_NAME,
        grid=GRID,
        mode=CONFIG.get('transfer_mode', 'copy'),
        data_paths=DATA_PATHS,
        ensemble=ENSEMBLE,
        overwrite=overwrite)
    if num_moved == -1:
        return 1

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
        pbar = tqdm(
            desc="Generating mapfiles",
            total=num_moved)
        res = mapfile_gen(
            basepath=BASEOUTPUT,
            inipath=INIPATH,
            experiment=EXPERIMENT_NAME,
            maxprocesses=NUMWORKERS,
            event=event,
            pbar=pbar)
    except KeyboardInterrupt as error:
        print_message('Keyboard interrupt ... exiting')
        event.set()
    else:
        if res == 0:
            print_message('Publication prep complete', 'ok')
    return 0


if __name__ == "__main__":
    sys.exit(main())