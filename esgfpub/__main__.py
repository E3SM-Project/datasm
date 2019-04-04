"""
A tool for automating much of the ESGF publication process
"""

from esgfpub.util import structure_gen, transfer_files, mapfile_gen

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
        print_message("Unable to parse config file")
        print(repr(error))
        sys.exit(1)

    try:
        BASEOUTPUT = CONFIG['output_path']
        CASE = CONFIG['case']
        GRID = CONFIG['non_native_grid']
        ATMRES = CONFIG['atmospheric_resolution']
        OCNRES = CONFIG['ocean_resolution']
        DATA_PATHS = CONFIG['data_paths']
        ENSEMBLE = CONGIG['ensemble']
    except ValueError as error:
        print_message('Unable to find values in config file')
        print(repr(error))
        sys.exit(1)

    try:
        print_message('Generating ESGF file structure', 'ok')
        structure_gen(
            basepath=BASEOUTPUT,
            casename=CASE,
            grid=GRID,
            atmos_res=ATMRES,
            ocean_res=OCNRES,
            data_paths=DATA_PATHS,
            ensemble=ENSEMBLE)
    except IOError as error:
        print_message('Error generating file structure')
        print(repr(error))
        sys.exit(1)

    print_message('Transfering files', 'ok')
    ret = transfer_files(
        outpath=BASEOUTPUT,
        case=CASE,
        grid=GRID,
        mode=CONFIG.get('transfer_mode', 'copy'),
        data_paths=DATA_PATHS,
        ensemble=ENSEMBLE)
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
            casename=CASE,
            maxprocesses=NUMWORKERS,
            event=event)
    except KeyboardInterrupt as error:
        print_message('Keyboard interrupt ... exiting')
        event.set()
    else:
        if res == 0:
            print_message('Publication prep complete', 'ok')
