"""
Utility functions for esgfpub
"""

import os
import sys
from subprocess import call, Popen, PIPE
from shutil import move, copy
from time import sleep
from tqdm import tqdm


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
        print(colors.FAIL + '[-] ' + colors.ENDC +
              colors.BOLD + str(message) + colors.ENDC)
    elif status == 'ok':
        print(colors.OKGREEN + '[+] ' + colors.ENDC + str(message))


def makedir(directory):
    """
    Make a directory if it doesnt already exist
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


def transfer_files(outpath, experiment, mode, grid, data_paths, ensemble, overwrite):
    """
    Move or copy data into the ESGF publication structure

    Parameters
    ----------
        outpath (str): the base of the ESGF publication structure
        mode (str): either 'move' or 'copy'
        experiment (str): the name of the experiment being published
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

    for dtype, path in list(data_paths.items()):
        contents = os.listdir(path)
        for item in tqdm(contents, desc=dtype):
            src = os.path.join(path, item)
            dst = setup_dst(
                experiment=experiment,
                basepath=outpath,
                res_dir=resolution_dir,
                grid=grid,
                datatype=dtype,
                filename=item,
                ensemble=ensemble)
            tail, _ = os.path.split(dst)
            if not os.path.exists(tail):
                os.makedirs(tail)
            if os.path.exists(dst):
                if overwrite:
                    os.remove(dst)
                else:
                    continue
            if not os.path.exists(src):
                print_message('{} does not exist'.format(src))
                continue
            try:
                transfer(src, dst)
            except OSError as error:
                print(src, dst)
                print(repr(error))
                return -1
    return 0


def mapfile_gen(basepath, inipath, experiment, maxprocesses, event=None):
    """
    Generate mapfiles for ESGF

    Parameters
    ----------
        basepath (str): the base of the data, the case directory should be below this
        inipath (str): path to directory with ini files
        experiment (str): the name of the experiment to generate mapfiles for
        maxprocesses (str): the number of processes to use for hashing
        event (threading.Event): an event to terminate the process early
    """
    outpath = os.path.join(basepath, '{}_mapfiles'.format(experiment))
    datapath = os.path.join(basepath, experiment)
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
        lines = proc.stdout.readlines()
        if lines != b'':
            for line in lines:
                print(line)
        sleep(1)
    err = proc.stderr.readlines()
    if err:
        for line in err:
            print(line)
        return -1
    else:
        return 0


def setup_dst(experiment, basepath, res_dir, grid, datatype, filename, ensemble):
    """
    Find the destination path for a file
    """
    freq = 'mon'
    dstgrid = 'native'
    if datatype in ['atmos', 'atmos_regrid', 'atmos_ts', 'atmos_daily']:
        type_dir = 'atmos'
        if datatype == 'atmos':
            output_type = 'model-output'
        elif datatype == 'atmos_daily':
            freq = 'day'
            output_type = 'model-output'
        elif datatype == 'atmos_ts':
            output_type = 'time-series'
            dstgrid = grid
        elif datatype == 'atmos_regrid':
            output_type = 'model-output'
            dstgrid = grid
    elif datatype in ['land', 'land_regrid']:
        if datatype == 'land_regrid':
            dstgrid = grid
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
    elif 'atmos_climo_' in datatype:
        type_dir = 'atmos'
        output_type = 'climo'
        dstgrid = grid
        freq = 'monClim'
        for season in ['ANN', 'DJF', 'MAM', 'JJA', 'SON']:
            if season in filename:
                freq = 'seasonClim'
                break

        idx = len('atmos_climo_')
        year_length = datatype[idx:]
        freq += '-{}'.format(year_length)
    else:
        raise Exception('{} is an invalid data type'.format(datatype))

    return os.path.join(
        basepath,
        experiment,
        res_dir,
        type_dir,
        dstgrid,
        output_type,
        freq,
        ensemble,
        'v1',
        filename)
