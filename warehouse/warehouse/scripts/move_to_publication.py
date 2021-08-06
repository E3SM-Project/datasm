import sys
import os
import argparse
from pathlib import Path
from shutil import rmtree
from subprocess import Popen, PIPE
from warehouse.util import con_message


def parse_args():
    parser = argparse.ArgumentParser(
        description="Move all files from given source directory to given destination directory.  "
        'Move parent source directory ".mapfile", if it exists, to the parent destination directory, '
        'along with the corresponding ".status" file.'
    )
    parser.add_argument(
        "--src-path",
        type=str,
        dest="src",
        required=True,
        help="source directory of netCDF files to be moved",
    )
    parser.add_argument(
        "--dst-path",
        type=str,
        dest="dst",
        required=True,
        help="destination directory for netCDF files to be moved",
    )
    return parser.parse_args()


def validate_args(args):
    """
    Ensure the src path exists and is not empty.
    Ensure the dst path exists and is empty.
    """
    src_path = Path(args.src)
    dst_path = Path(args.dst)
    if not src_path.exists() or not src_path.is_dir():
        con_message("error", "Source directory does not exist or is not a directory")
        return False
    if not any(src_path.iterdir()):
        con_message("error", "Source directory is empty")
        return False

    if not dst_path.exists() or not dst_path.is_dir():
        dst_path.mkdir(parents=True, exist_ok=True)
    if any(dst_path.iterdir()):
        con_message("error", "Destination directory is not empty")
        return False

    return True


def conduct_move(args, move_method="none"):
    if move_method == "none":
        con_message("error","Move_to_Publication: Must set move_method to 'move' or to 'link'")
        return 1

    src_path = Path(args.src)
    dst_path = Path(args.dst)

    file_count = 0
    for afile in src_path.glob("*.nc"):  # all .nc files
        destination = dst_path / afile.name
        if destination.exists():
            con_message(
                "error",
                f"Trying to move file {afile} to {destination}, but the destination already exists",
            )
            sys.exit(1)
        if move_method == "move":
            afile.replace(destination)
        else:
            src_target = src_path / afile.name
            # make symlink like ln -s src_target destination, but with Path('the-link-you-want-to-create').symlink_to('the-original-file')
            Path(destination).symlink_to(src_target)
        file_count += 1

    con_message("info", f"moved {file_count} files from {src_path} to {dst_path}")

    mapfile = next(src_path.parent.glob("*.map"))
    with open(mapfile, "r") as instream:
        dataset_id = instream.readline().split("|")[0].strip().split("#")[0]
    dst = Path(dst_path.parent, f"{dataset_id}.map")
    con_message("info", f"Moving the mapfile to {dst}")
    mapfile.replace(dst)

    message = f"mapfile_path={dst},pub_name={dst_path.name},ware_name={src_path.name}"
    if messages_path := os.environ.get("message_file"):
        with open(messages_path, "w") as outstream:
            outstream.write(message)
    else:
        con_message("error", message)

    return 0


def main():
    parsed_args = parse_args()
    src_path = Path(parsed_args.src)
    dst_path = Path(parsed_args.dst)
    move_method = "move"
    if src_path == dst_path:
        move_method = "link"
        message = f"mapfile_path={next(src_path.parent.glob('*.map'))},pub_name={dst_path.name},ware_name={src_path.name}"
        if messages_path := os.environ.get("message_file"):
            with open(messages_path, "w") as outstream:
                outstream.write(message)
        else:
            con_message("error", message)
        sys.exit(0)

    if not validate_args(parsed_args):
        sys.exit(1)

    return conduct_move(parsed_args,move_method)


if __name__ == "__main__":
    sys.exit(main())
