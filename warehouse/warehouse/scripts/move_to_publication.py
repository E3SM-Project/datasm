import sys
import os
import argparse
from pathlib import Path
from shutil import rmtree
from subprocess import Popen, PIPE


def parse_args():
    parser = argparse.ArgumentParser(
        description="Move all files from given source directory to given destination directory.  "
                    "Move parent source directory \".mapfile\", if it exists, to the parent destination directory, "
                    "along with the corresponding \".status\" file.")
    parser.add_argument(
        '--src-path',
        type=str,
        dest='src',
        required=True,
        help="source directory of netCDF files to be moved")
    parser.add_argument(
        '--dst-path',
        type=str,
        dest='dst',
        required=True,
        help="destination directory for netCDF files to be moved")
    return parser.parse_args()


def validate_args(args):
    ''' 
    Ensure the src path exists and is not empty.
    Ensure the dst path exists and is empty.
    '''
    src_path = Path(args.src)
    dst_path = Path(args.dst)
    if not src_path.exists() or not src_path.is_dir():
        # raise ValueError("Source directory does not exist or is not a directory")
        print("Source directory does not exist or is not a directory")
        return False
    if not any(src_path.iterdir()):
        # raise ValueError("Source directory is empty")
        print("Source directory is empty")
        return False

    if not dst_path.exists() or not dst_path.is_dir():
        dst_path.mkdir(parents=True, exist_ok=True)
    if any(dst_path.iterdir()):
        # raise ValueError("Destination directory is not empty")
        print("Destination directory is not empty")
        return False

    return True


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

    if s_path == d_path:
        # need a special action for robustness in this case (upgrade publication)
        logging.error(f'Cannot determine statusfile atomicity - src.parent == dst.parent')
        return ''

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
    

def conduct_move(args):
    src_path = Path(args.src)
    dst_path = Path(args.dst)

    for afile in src_path.glob('*.nc'):  # all .nc files
        destination = dst_path / afile.name
        if destination.exists():
            raise ValueError(
                f"Trying to move file {afile} to {destination}, but the destination already exists")
        afile.replace(destination)
    
    for mapfile in src_path.parent.glob(".mapfile"):
        with open(mapfile, 'r') as instream:
            dataset_id = instream.readline().split('|')[0].strip().split('#')[0]
        dst = Path(dst_path.parent, f"{dataset_id}.map")
        print(f"Moving the mapfile to {dst}")
        mapfile.replace(dst)
    
    consolidate_statusfile_location(src_path.parent, dst_path.parent)
    
    message = f"mapfile_path={dst},pub_name={dst_path.name},ware_name={src_path.name}"
    if (messages_path := os.environ.get('message_file')):
        with open(messages_path, 'w') as outstream:
            outstream.write(message)
    else:
        print(message)

    return 0


def main():
    parsed_args = parse_args()

    if not validate_args(parsed_args):
        sys.exit(1)

    return conduct_move(parsed_args)


if __name__ == "__main__":
    sys.exit(main())
