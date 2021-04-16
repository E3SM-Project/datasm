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

def consolidate_statusfile_to(dsid, loc, w_root, p_root):
    '''
        Convert dsid to full warehouse and publication statusfile paths.
        If none exists, take no action and return an ERROR message(?)
        If only one exists, and it is already in "loc", return path.
        If only one exists, and it is not in the "loc", move it there, return path.
        If both exist, sort-merge the lines of both to "loc", delete the other, return path.
    '''
    ds_part = dsid.replace('.'.os.sep)
    w_path = os.path.join(w_root,ds_part,'.status')
    p_path = os.path.join(p_root,ds_part,'.status')
    have_w = os.path.exists(w_path)
    have_p = os.path.exists(p_path)
    if not have_w and not have_p:
        logging.error(f'No status file can be found for dataset_id {dsid}')
        return ''
        # return 'ERROR:NO_STATUS_FILE_PATH'
    if (have_w != have_p):
        if (have_w and loc = 'W'):
            return w_path
        if (have_p and loc = 'P'):
            return p_path
        if (have_w and loc = 'P'):
            subprocess.run(['mv', w_path, p_path])
            return p_path
        if (have_p and loc = 'W'):
            subprocess.run(['mv', p_path, w_path])
            return w_path
    # must consolidate two status files
    w_list = loadFileLines(w_path)
    p_list = loadFileLines(p_path)
    s_list = w_list + p_list
    s_list.sort()
    if loc == 'W':
        print_file_list(w_path,s_list)
        subprocess.run(['rm', '-f', p_path])
        return w_path
    if loc == 'P':
        print_file_list(p_path,s_list)
        subprocess.run(['rm', '-f', w_path])
        return p_path
    
    logging.error(f'Unrecognized loc specifier: {loc}');
    return ''
    # return f'ERROR: unrecognized loc specifier: {loc}'

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
