"""
A module of utilities for automating the ESGF publciation process
"""
import os
from pathlib import Path
from fnmatch import fnmatch
from subprocess import call, Popen, PIPE
from shutil import move, copy
from time import sleep


def structure_gen(basepath, casename, grids, atmos_res, ocean_res, file_types, *args, **kwargs):
    """
    generate the esgf publication structure

    Parameters
    ----------
        casename (str): the name of the run
        grids (list(str)): any grids in addition to native that are being published
        atmos_res (str): the atmospheric resolution i.e. 1deg
        ocean_res (str): the ocean resolution i.e. 60-30km
        file_types (dict): a dictionary with keys with the file type name, and values of the
            glob to use to match those files
    """

    # make the top level directories
    
    resolution_dir = os.path.join(
        basepath,
        casename,
        '{atm_res}_atm_{ocn_res}_ocean'.format(
            atm_res=atmos_res,
            ocn_res=ocean_res))
    makedir(resolution_dir)

    # iterate over the file types and create required subdirectories
    for dtype in file_types.keys():
        # /basedir/resolution_dir/dtype
        dtype_dir = os.path.join(resolution_dir, dtype)
        # all data types have a 'native' grid type
        grid_dir = os.path.join(dtype_dir, 'native')
        # /basedir/resolution_dir/dtype/native/model-output/mon/ens1/v1
        makedir(
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
                makedir(os.path.join(
                    grid_dir,
                    'model-output',
                    'mon',
                    'ens1',
                    'v1'))
                if dtype == 'atmos' and grid != 'native':
                    # /basedir/resolution_dir/dtype/grid/climo/monClim/ens1/v1
                    makedir(
                        os.path.join(
                            grid_dir,
                            'climo',
                            'monClim',
                            'ens1',
                            'v1'))
                    # /basedir/resolution_dir/dtype/grid/climo/seasonClim/ens1/v1
                    makedir(
                        os.path.join(
                            grid_dir,
                            'climo',
                            'seasonClim',
                            'ens1',
                            'v1'))
                    # /basedir/resolution_dir/dtype/grid/time-series/mon/ens1/v1
                    makedir(
                        os.path.join(
                            grid_dir,
                            'time-series',
                            'mon',
                            'ens1',
                            'v1'))
        # set the permissions so the ESGF server can open the directories
        cmd = ['chmod', '-R', 'a+rx', os.path.join(basepath, casename)]
        call(cmd)

def makedir(directory):
    """
    Make a directory if it doesnt already exist
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

def collect_files(input_path, file_types, excludes):
    """
    Walks a directory tree to find files, doesnt go down any directory that matches a pattern in excludes 
    
    Parameters
    ----------
        input_path (str): root of search tree
        file_types (dict): a map of file_type names to globs that match that type
        excludes (list): a list of strings of directory names to avoid searching in
    Returns
    -------
        a dict of lists, the keys are file_types with non-zero files that match, the list
            is a list of paths to files of that type that were found in the tree
    """
    datafiles = dict()
    exclude = set(excludes)
    for root, dirs, files in os.walk(input_path):
        dirs[:] = set(dirs) - exclude
        for file in files:
            for ftype in file_types.keys():
                if fnmatch(file, file_types[ftype]):
                    if not datafiles.get(ftype):
                        datafiles[ftype] = list()
                    path = Path(root, file).absolute()
                    datafiles[ftype].append(path)
                    break
    return datafiles

def transfer_files(basepath, mode, case, file_types, excludes):
    """
    Move or copy data into the ESGF publication structure

    Parameters
    ----------
        basepath (str): the base of the ESGF publication structure
        mode (str): either 'move' or 'copy'
        case (str): the full name of the case
s       file_types (list): List of tuples of (file_type(str), regex that matches files of this type(str))
    Returns
    -------
        True if all data is moved/copied successfuly
        False otherwise
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
    resolution_dir = os.listdir(os.listdir(basepath)[0])[0]

    """
    walk over the raw input file directory and the output/pp directories
    collecting all the files we're meant to publish, then transfer them to
    the destination
    """
    files = collect_files(basepath, file_types, excludes)


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
           '--max-processes', maxprocesses,
           'datapath']
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    while proc.poll() is None:
        if event is not None and event.is_set():
            proc.terminate()
        sleep(1)


def _setup_dst(short_name, basepath, res_dir, grid, datatype, filename):
    """"""
    freq = 'mon'
    if datatype in ['atm', 'atm_regrid', 'atm_ts_regrid', 'climo_regrid']:
        type_dir = 'atmos'
        if datatype == 'atm':
            output_type = 'model-output'
        elif datatype == 'atm_ts_regrid':
            output_type = 'time-series'
        elif datatype == 'climo_regrid':
            output_type = 'climo'
            freq = 'monClim'
            for season in ['ANN', 'DJF', 'MAM', 'JJA', 'SON']:
                if season in filename:
                    freq = 'seasonClim'
                    break
    elif datatype in ['lnd', 'lnd_regrid']:
        type_dir = 'land'
        output_type = 'model-output'
    elif datatype == 'ocn':
        type_dir = 'ocean'
        output_type = 'model-output'
    elif datatype == 'ice':
        type_dir = 'sea-ice'
        output_type = 'model-output'
    else:
        raise Exception('{} is an invalid data type'.format(datatype))

    return os.path.join(
        basepath,
        short_name,
        res_dir,
        type_dir,
        grid,
        output_type,
        freq,
        'ens1',
        'v1',
        filename)
