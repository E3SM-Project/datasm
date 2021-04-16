import sys
import json
import traceback
import logging

from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
from pathlib import Path

def load_file_lines(file_path):
    if not file_path:
        return list()
    file_path = Path(file_path)
    if not file_path.exists() or not file_path.is_file():
        raise ValueError(f"file at path {file_path.resolve()} either doesnt exist or is not a regular file")
    with open(file_path, "r") as instream:
        retlist = [[i for i in x.split('\n') if i].pop() for x in instream.readlines() if x[:-1]]
    return retlist

def get_last_status_line(file_path):
    with open(file_path, 'r') as instream:
        last_line = None
        for line in instream.readlines():
            if "STAT" in line:
                last_line = line
        return last_line
# -----------------------------------------------

def print_list(prefix, items):
    for x in items:
        print(f'{prefix}{x}')
# -----------------------------------------------

def print_file_list(outfile, items):
    with open(outfile, 'w') as outstream:
        for x in items:
            outstream.write(f"{x}\n")
# -----------------------------------------------

def print_debug(e):
    """
    Print an exceptions relevent information
    """
    print('1', e.__doc__)
    print('2', sys.exc_info())
    print('3', sys.exc_info()[0])
    print('4', sys.exc_info()[1])
    _, _, tb = sys.exc_info()
    print('5', traceback.print_tb(tb))
# -----------------------------------------------

def consolidate_statusfile_location(src_path, dst_path):
    '''
        Seek .status file in parent directories.
        If none exists, take no action and return an ERROR message(?)
        If only one exists, and it is already in "loc", return path.
        If only one exists, and it is not in the "loc", move it there, return path.
        If both exist, sort-merge the lines of both to "loc", delete the other, return path.
    '''
    s_path = os.path.join(src_path.parent,'.status')
    d_path = os.path.join(dst_path.parent,'.status')
    have_s = os.path.exists(s_path)
    have_d = os.path.exists(d_path)
    if not have_s and not have_d:
        logging.error(f'No status file can be found for dataset')
        return ''
        # return 'ERROR:NO_STATUS_FILE_PATH'
    if have_s != have_d:
        if have_s:
            subprocess.run(['mv', s_path, d_path])
        return d_path
    # must consolidate two status files
    s_list = loadFileLines(s_path)
    d_list = loadFileLines(d_path)
    f_list = s_list + d_list
    f_list.sort()
    print_file_list(d_path, f_list)
    return d_path
    

def sproket_with_id(dataset_id, sproket_path='sproket', **kwargs):

    # create the path to the config, write it out
    tempfile = NamedTemporaryFile(suffix='.json')
    with open(tempfile.name, mode='w') as tmp:
        config_string = json.dumps({
            'search_api': "https://esgf-node.llnl.gov/esg-search/search/",
            'data_node_priority': ["esgf-data2.llnl.gov", "aims3.llnl.gov", "esgf-data1.llnl.gov"],
            'fields': {
                'dataset_id': dataset_id,
                'latest': 'true'
            }
        })

        tmp.write(config_string)
        tmp.seek(0)

        cmd = [sproket_path, '-config', tempfile.name, '-y', '-urls.only']
        proc = Popen(cmd, shell=False, stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
    if err:
        print(err.decode('utf-8'))
        return dataset_id, None

    files = sorted([i.decode('utf-8') for i in out.split()])
    return dataset_id, files
# -----------------------------------------------
