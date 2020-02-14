"""
Utility functions for esgfpub
"""

import os
import sys
import argparse
from subprocess import call, Popen, PIPE
from shutil import move, copy
from time import sleep
from tqdm import tqdm
from esgfpub import resources


def parse_args():
    parser = argparse.ArgumentParser(prog='esgfpub')
    subparsers = parser.add_subparsers(
        title='subcommands',
        description='valid subcommands',
        dest='subparser_name')

    parser_publish = subparsers.add_parser(
        'publish', help='Move data and generate mapfiles')
    parser_publish.add_argument(
        "config",
        help="Path to configuration file")
    parser_publish.add_argument(
        "-t",
        "--transfer-mode",
        default='link',
        help="the file transfer mode, allowed values are link, move, or copy")
    parser_publish.add_argument(
        '--over-write',
        help="Over write any existing files",
        action='store_true')
    parser_publish.add_argument(
        '-o',
        '--output-mapfiles',
        dest='mapout',
        help='The output location for mapfiles, defaults to ./mapfiles/',
        default='./mapfiles')
    parser_publish.add_argument(
        '--debug',
        action="store_true")

    parser_esgf_check = subparsers.add_parser(
        'check', help='Check the file structure and ESGF database for datasets')
    parser_esgf_check.add_argument(
        '-d',
        '--data-path',
        help="path to the root data directory containing the data",
        default='/p/user_pub/work')
    tail, _ = os.path.split(resources.__file__)
    parser_esgf_check.add_argument(
        '--case-spec',
        default=os.path.join(tail, 'dataset_spec.yaml'),
        help="path to yaml file containing the case spec")
    parser_esgf_check.add_argument(
        '-p',
        '--project',
        help='Which project to check for, valid arguments are cmip6 or e3sm. Default is both')
    parser_esgf_check.add_argument(
        '-c',
        '--cases',
        nargs="+",
        default=['all'],
        help="Which case to check the data for, default is all")
    parser_esgf_check.add_argument(
        '-v',
        '--variables',
        nargs="+",
        default=['all'],
        help="Which variables to check for, default is all")
    parser_esgf_check.add_argument(
        '-t',
        '--tables',
        nargs="+",
        default=['all'],
        help="List of CMIP6 tables to search in, default is all")
    parser_esgf_check.add_argument(
        '--ens',
        nargs="+",
        default=['all'],
        help="List of ensemble members to check, default all")
    parser_esgf_check.add_argument(
        '-s',
        '--serial',
        action='store_true',
        help='Should this be run in serial or parallel')
    parser_esgf_check.add_argument(
        '--published',
        action="store_true",
        help="Check the LLNL ESGF node to see if the variables have been published")
    parser_esgf_check.add_argument(
        '--sproket',
        help='path to sproket, only needed if --published is turned on')
    parser_esgf_check.add_argument(
        '-m',
        '--max-connections',
        type=int,
        default=5,
        help="Maximum number of simultanious connections to ESGF node")
    parser_esgf_check.add_argument(
        '--debug',
        action="store_true")
    return parser.parse_args(sys.argv[1:])


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


def get_atm_casename(filename):
    i = filename.index(".cam.h0")
    if i == -1:
        return -1
    return filename[:i]


def get_lnd_casename(filename):
    i = filename.index(".clm2.h0")
    if i == -1:
        return -1
    return filename[:i]


def validate_raw(data_paths, start, end):
    """
    Checks that the atmos, land, sea-ice, and ocean raw files are present
    returns True if all files are found, False otherwise
    """
    missing = False
    if 'atmos' in data_paths:
        files = sorted(os.listdir(data_paths['atmos']))
        if not files:
            print("no atm files found")
        else:
            casename = get_atm_casename(files[0])
            if casename == -1:
                raise ValueError(
                    "Unable to find casename from {}".format(files[0]))
            for year in range(start, end + 1):
                for month in range(1, 13):
                    name = "{}.cam.h0.{:04d}-{:02d}.nc".format(
                        casename, year, month)
                    if name not in files:
                        print("{} is missing".format(name))
                        missing = True

    if "land" in data_paths:
        files = sorted(os.listdir(data_paths['land']))
        if not files:
            print("no land files found")
        else:
            casename = get_lnd_casename(files[0])
            if casename == -1:
                raise ValueError(
                    "Unable to find casename from {}".format(files[0]))
            for year in range(start, end + 1):
                for month in range(1, 13):
                    name = "{}.clm2.h0.{:04d}-{:02d}.nc".format(
                        casename, year, month)
                    if name not in files:
                        print("{} is missing".format(name))
                        missing = True

    if "sea-ice" in data_paths:
        files = sorted(os.listdir(data_paths['sea-ice']))
        if not files:
            print("no ice files found")
        else:
            for year in range(start, end + 1):
                for month in range(1, 13):
                    name = "mpascice.hist.am.timeSeriesStatsMonthly.{:04d}-{:02d}-01.nc".format(
                        year, month)
                    if name not in files:
                        print("{} is missing".format(name))
                        missing = True

    if "ocean" in data_paths:
        files = sorted(os.listdir(data_paths['ocean']))
        if not files:
            print("no ocn files found")
        else:
            for year in range(start, end + 1):
                for month in range(1, 13):
                    name = "mpaso.hist.am.timeSeriesStatsMonthly.{:04d}-{:02d}-01.nc".format(
                        year, month)
                    if name not in files:
                        print("{} is missing".format(name))
                        missing = True

    if missing:
        return False
    else:
        return True


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
        number of files transfer if everything completed successfully
        -1 on error
    """
    if mode not in ['copy', 'move', 'link']:
        raise ValueError('{} is not a supported mode'.format(mode))
    if mode == 'move':
        transfer = move
    elif mode == 'link':
        transfer = os.symlink
    else:
        transfer = copy

    resolution_dir = os.listdir(os.path.join(outpath, experiment))[0]
    num_transfered = 0

    for dtype, path in list(data_paths.items()):
        contents = os.listdir(path)

        for _, item in enumerate(tqdm(contents, desc="{}: ".format(dtype))):

            src = os.path.join(path, item)
            dst = setup_dst(
                experiment=experiment,
                basepath=outpath,
                res_dir=resolution_dir,
                grid=grid,
                datatype=dtype,
                filename=item,
                ensemble=ensemble)
            num_transfered += 1
            if os.path.exists(dst):
                continue
            tail, _ = os.path.split(dst)
            if not os.path.exists(tail):
                os.makedirs(tail)

            if os.path.exists(dst) or os.path.lexists(dst):
                if overwrite:
                    os.remove(dst)
                else:
                    print("Skipping {}".format(dst))
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

    return num_transfered


def mapfile_gen(basepath, inipath, experiment, outpath, maxprocesses, pbar, event=None):
    """
    Generate mapfiles for ESGF

    Parameters
    ----------
        basepath (str): the base of the data, the case directory should be below this
        inipath (str): path to directory with ini files
        experiment (str): the name of the experiment to generate mapfiles for
        outpath (str): the path to were the mapfiles should be stored after generation
        maxprocesses (str): the number of processes to use for hashing
        event (threading.Event): an event to terminate the process early
        pbar (tqdm): a tqdm progressbar
    """
    datapath = os.path.join(basepath, experiment)
    cmd = ['esgmapfile', 'make',
           '--outdir', outpath,
           '-i', inipath,
           '--project', 'e3sm',
           '--max-processes', str(maxprocesses),
           '--debug',
           datapath]
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    while proc.poll() is None:
        if event is not None and event.is_set():
            proc.terminate()
        for line in proc.stdout:
            if "SUCCESS" in line.decode('utf-8'):
                pbar.update(1)
        for line in proc.err:
            print(line.decode('utf-8'))
    err = proc.stderr.readlines()
    pbar.close()
    if err:
        for line in err:
            print(line)
        return 1
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

    new_path = os.path.join(
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
    return new_path
