import sys
import traceback
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

def print_list(prefix, items):
    for x in items:
        print(f'{prefix}{x}')

def print_file_list(outfile, items):
    with open(outfile, 'w') as outstream:
        for x in items:
            outstream.write(f"{x}\n")

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