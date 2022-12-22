import sys
import os
import argparse
import re
from pathlib import Path
from shutil import rmtree
from subprocess import Popen, PIPE
from datasm.util import con_message


def parse_args():
    parser = argparse.ArgumentParser(
        description="Move all files from given source directory to given destination directory.  "
    )
    parser.add_argument(
        "--src-path",
        type=str,
        dest="src",
        required=True,
        help="source version directory of netCDF files to be moved",
    )
    parser.add_argument(
        "--dst-path",
        type=str,
        dest="dst",
        required=True,
        help="destination version directory for netCDF files to be moved",
    )
    return parser.parse_args()


def validate_args(args):
    """
    Ensure the src path (including vdir) exists and is not empty.
    Ensure the dst path (including vdir) exists and is empty.
    """
    src_path = Path(args.src)
    dst_path = Path(args.dst)
    if not src_path.exists() or not src_path.is_dir():
        con_message("error", "Source version directory does not exist or is not a directory")
        return False
    if not any(src_path.iterdir()):
        con_message("error", "Source version directory is empty")
        return False

    if not dst_path.exists() or not dst_path.is_dir():
        dst_path.mkdir(parents=True, exist_ok=True)
    if any(dst_path.iterdir()):
        con_message("error", "Destination version directory is not empty")
        return False

    return True

def conduct_move(args, move_method="none"):
    if move_method == "none":
        con_message("error","Move_to_Publication: Must set move_method to 'move' or to 'link'")
        return 1

    con_message("info",f"conduct_move: move_method  = {move_method}")

    src_path = Path(args.src)
    dst_path = Path(args.dst)

    # NOW move the files

    if "namefile" in str(src_path):
        glob_pattern = "*_in"
    elif "streams" in str(src_path):
        glob_pattern = "streams.*"
    else:
        glob_pattern = "*.nc"
    
    file_count = 0
    for sfile in src_path.glob(glob_pattern):
        destination = dst_path / sfile.name
        if destination.exists():
            con_message( "error", f"Trying to move file {sfile} to {destination}, but the destination already exists",)
            sys.exit(1)
        if move_method == "move":
            tfile = sfile.resolve()
            destination = dst_path / tfile.name
            tfile.replace(destination)
        else:
            src_target = src_path / sfile.name
            # make symlink like ln -s src_target destination, but with Path('the-link-you-want-to-create').symlink_to('the-original-file')
            Path(destination).symlink_to(src_target)
        file_count += 1

    con_message("info", f"moved {file_count} files from {src_path} to {dst_path}")

    return 0


def main():
    parsed_args = parse_args()

    if not validate_args(parsed_args):
        sys.exit(1)

    src_path = Path(parsed_args.src)
    dst_path = Path(parsed_args.dst)

    if src_path == dst_path:
        con_message("info", "move_to_publication: move elided; src is dst")
        sys.exit(0)

    src_parent, _ = os.path.split(src_path)
    dst_parent, _ = os.path.split(dst_path)

    move_method = "move"
    if src_parent == dst_parent:
        move_method = "link"

    con_message("info", f"calling conduct_move with method {move_method}")

    retcode = conduct_move(parsed_args, move_method)
    if retcode != 0:
        con_message("error", f"move_to_publication: return code = {retcode}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
