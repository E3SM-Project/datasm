"""
A module of utilities for automating the ESGF publciation process
"""
from __future__ import print_function
import os
from subprocess import call, Popen, PIPE
from shutil import move, copy
from time import sleep
from tqdm import tqdm


def structure_gen(basepath, casename, grid, atmos_res, ocean_res, data_paths):
    """
    generate the esgf publication structure

    Parameters
    ----------
        casename (str): the name of the run
        grids (list(str)): any grids in addition to native that are being published
        atmos_res (str): the atmospheric resolution i.e. 1deg
        ocean_res (str): the ocean resolution i.e. 60-30km
        data_paths (dict): a dictionary with keys with the file type name, and values of the
            path to where those files are stored
    """

    # make the top level directories
    resolution_dir = os.path.join(
        basepath,
        casename,
        '{atm_res}_atm_{ocn_res}_ocean'.format(
            atm_res=atmos_res,
            ocn_res=ocean_res))

    # make the list of descrete types to handle
    dtypes = list()
    for dtype in data_paths.keys():
        index = dtype.find('_')
        if index > 0:
            new_type = dtype[:index]
        else:
            new_type = dtype
        if new_type not in dtypes:
            dtypes.append(new_type)

    grids = ['native', grid]
    new_paths = list()
    # iterate over the file types and create required subdirectories
    for dtype in dtypes:
        # /basedir/resolution_dir/dtype
        dtype_dir = os.path.join(resolution_dir, dtype)
        # all data types have a 'native' grid type
        grid_dir = os.path.join(dtype_dir, 'native')
        # /basedir/resolution_dir/dtype/native/model-output/mon/ens1/v1
        new_paths.append(
            os.path.join(
                grid_dir,
                'model-output',
                'mon',
                'ens1',
                'v1'))
        # atmos and land types should include climos/regrid/ts
        if dtype in ['atmos', 'land']:
            for grid in grids:
                grid_dir = os.path.join(dtype_dir, grid)
                # /basedir/resolution_dir/dtype/grid/model-output/mon/ens1/v1
                new_paths.append(
                    os.path.join(
                        grid_dir,
                        'model-output',
                        'mon',
                        'ens1',
                        'v1'))
                if dtype == 'atmos' and grid != 'native':
                    # /basedir/resolution_dir/dtype/grid/climo/monClim/ens1/v1
                    new_paths.append(
                        os.path.join(
                            grid_dir,
                            'climo',
                            'monClim',
                            'ens1',
                            'v1'))
                    # /basedir/resolution_dir/dtype/grid/climo/seasonClim/ens1/v1
                    new_paths.append(
                        os.path.join(
                            grid_dir,
                            'climo',
                            'seasonClim',
                            'ens1',
                            'v1'))
                    # /basedir/resolution_dir/dtype/grid/time-series/mon/ens1/v1
                    new_paths.append(
                        os.path.join(
                            grid_dir,
                            'time-series',
                            'mon',
                            'ens1',
                            'v1'))
    for path in tqdm(new_paths):
        makedir(path)
    # set the permissions so the ESGF server can open the directories
    cmd = ['chmod', '-R', 'a+rx', os.path.join(basepath, casename)]
    call(cmd)


def makedir(directory):
    """
    Make a directory if it doesnt already exist
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


def transfer_files(outpath, case, mode, grid, data_paths):
    """
    Move or copy data into the ESGF publication structure

    Parameters
    ----------
        outpath (str): the base of the ESGF publication structure
        mode (str): either 'move' or 'copy'
        case (str): the case being published
        grid (str): the non-native grid name
        data_paths (dict): a dictionary with keys with the file type name, and values of the
            path to where those files are stored
    Returns
    -------
        0 if everything completed successfully
        -1 on error
    """
    if mode not in ['copy', 'move', 'link']:
        raise Exception('{} is not a supported mode'.format(mode))
    if mode == 'move':
        transfer = move
    elif mode == 'link':
        transfer = os.symlink
    else:
        transfer = copy

    # the first subdirectory is a directory with the name
    # of the atm resolution and the ocean resolution
    resolution_dir = os.listdir(
        os.path.join(outpath, 
            os.listdir(outpath)[0]))[0]
    if not resolution_dir:
        raise Exception('Missing resolution directory')

    for dtype, path in data_paths.items():
        contents = os.listdir(path)
        for item in tqdm(contents, desc=dtype):
            src = os.path.join(path, item)
            dst = _setup_dst(
                case=case,
                basepath=outpath,
                res_dir=resolution_dir,
                grid=grid,
                datatype=dtype,
                filename=item)
            if os.path.exists(dst):
                continue
            if not os.path.exists(src):
                print_message('{} does not exist'.format(src))
                return -1
            try:
                transfer(src, dst)
            except OSError as error:
                print(src, dst)
                print(repr(error))
                return -1
    return 0

def mapfile_gen(basepath, inipath, casename, maxprocesses, event=None):
    """
    Generate mapfiles for ESGF

    Parameters
    ----------
        basepath (str): the base of the data, the case directory should be below this
        inipath (str): path to directory with ini files
        casename (str): the name of the case to generate mapfiles for
        maxprocesses (str): the number of processes to use for hashing
        event (threading.Event): an event to terminate the process early
    """
    outpath = os.path.join(basepath, '{}_mapfiles'.format(casename))
    datapath = os.path.join(basepath, casename)
    cmd = ['esgmapfile', 'make',
           '--outdir', outpath,
           '-i', inipath,
           '--project', 'e3sm',
           '--max-processes', str(maxprocesses),
           datapath]
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    while proc.poll() is None:
        if event is not None and event.is_set():
            proc.terminate()
        sleep(1)


def _setup_dst(case, basepath, res_dir, grid, datatype, filename):
    """
    Find the destination path for a file
    """
    freq = 'mon'
    if datatype in ['atmos', 'atmos_regrid', 'atmos_ts', 'atmos_climo']:
        type_dir = 'atmos'
        if datatype == 'atmos':
            output_type = 'model-output'
        elif datatype == 'atmos_ts':
            output_type = 'time-series'
        elif datatype == 'atmos_climo':
            output_type = 'climo'
            freq = 'monClim'
            for season in ['ANN', 'DJF', 'MAM', 'JJA', 'SON']:
                if season in filename:
                    freq = 'seasonClim'
                    break
    elif datatype in ['land', 'land_regrid']:
        type_dir = 'land'
        output_type = 'model-output'
    elif datatype == 'ocean':
        type_dir = 'ocean'
        output_type = 'model-output'
        grid = 'native'
    elif datatype == 'sea-ice':
        type_dir = 'sea-ice'
        output_type = 'model-output'
        grid = 'native'
    else:
        raise Exception('{} is an invalid data type'.format(datatype))

    return os.path.join(
        basepath,
        case,
        res_dir,
        type_dir,
        grid,
        output_type,
        freq,
        'ens1',
        'v1',
        filename)

class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_message(message, status='error'):
    """
    Prints a message with either a green + or a red -

    Parameters:
        message (str): the message to print
        status (str): th"""
    if status == 'error':
        print(colors.FAIL + '[-] ' + colors.ENDC + colors.BOLD + str(message) + colors.ENDC)
    elif status == 'ok':
        print(colors.OKGREEN + '[+] ' + colors.ENDC + str(message))