import os
import yaml
from threading import Event
from tqdm import tqdm
from esgfpub.util import print_message
from esgfpub.util import transfer_files, mapfile_gen, validate_raw, makedir


def stage(ARGS):

    debug = ARGS.debug

    if ARGS.over_write:
        overwrite = True
    else:
        overwrite = False

    try:
        with open(ARGS.config, 'r') as ip:
            CONFIG = yaml.load(ip, Loader=yaml.SafeLoader)
    except SyntaxError as error:
        print_message("Unable to parse config file, is it valid yaml?")
        print(repr(error))
        return 1

    try:
        BASEOUTPUT = CONFIG['output_path']
        MODEL_VERSION = CONFIG['model_version']
        ATMRES = CONFIG['atmospheric_resolution']
        OCNRES = CONFIG['ocean_resolution']
        DATA_PATHS = CONFIG['data_paths']
        ENSEMBLE = CONFIG['ensemble']
        EXPERIMENT_NAME = CONFIG['experiment']
        GRID = CONFIG.get('non_native_grid')
        START = int(CONFIG['start_year'])
        END = int(CONFIG['end_year'])
    except ValueError as error:
        print_message('Unable to find values in config file')
        print(repr(error))
        return 1

    print_message('Validating raw data', 'ok')
    if not validate_raw(DATA_PATHS, START, END):
        return 1

    base_path = os.path.join(BASEOUTPUT, MODEL_VERSION)

    resdirname = "{}_atm_{}_ocean".format(ATMRES, OCNRES)
    makedir(os.path.join(base_path, EXPERIMENT_NAME, resdirname))

    transfer_mode = ARGS.transfer_mode
    if transfer_mode == 'move':
        print_message('Moving files', 'ok')
    elif transfer_mode == 'copy':
        print_message('Copying files', 'ok')
    elif transfer_mode == 'link':
        print_message('Linking files', 'ok')
    num_moved, paths = transfer_files(
        outpath=base_path,
        experiment=EXPERIMENT_NAME,
        grid=GRID,
        mode=transfer_mode,
        data_paths=DATA_PATHS,
        ensemble=ENSEMBLE,
        overwrite=overwrite)
    if num_moved == -1:
        return 1

    RUNMAPS = CONFIG.get('mapfiles', False)
    if not RUNMAPS or RUNMAPS not in [True, 'true', 'True', 1, '1']:
        print_message('Not running mapfile generation', 'ok')
        print_message('Publication prep complete', 'ok')
        return 0
    else:
        print_message('Starting mapfile generation', 'ok')

    try:
        INIPATH = CONFIG['ini_path']
        MAPOUT = ARGS.mapout
    except:
        raise ValueError(
            "Mapfiles generation is turned on, but the config is missing the ini_path option")
    NUMWORKERS = CONFIG.get('num_workers', 4)
    event = Event()

    pbar = tqdm(
        desc="Generating mapfiles",
        total=num_moved)
    res = -1
    try:
        for path in paths:
            res = mapfile_gen(
                basepath=path,
                inipath=INIPATH,
                outpath=MAPOUT,
                maxprocesses=NUMWORKERS,
                env_name=ARGS.mapfile_env,
                debug=debug,
                event=event,
                pbar=pbar)
        pbar.close()
    except KeyboardInterrupt as error:
        print_message('Keyboard interrupt caught, exiting')
        event.set()
        return 1
    else:
        if res == 0:
            print_message('Publication prep complete', 'ok')
        else:
            print_message(
                'mapfile generation exited with status: {}'.format(res), 'error')
        return res
