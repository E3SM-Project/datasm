"""Top-level package for datasm."""
import argparse
from argparse import RawTextHelpFormatter


def parse_args(arg_sources, checkers):
    DESC = "Automated E3SM Data State Machine utilities"
    parser = argparse.ArgumentParser(
        prog="datasm",
        description=DESC,
        prefix_chars="-",
        formatter_class=RawTextHelpFormatter,
    )

    subcommands = parser.add_subparsers(
        title="subcommands", description="datasm subcommands", dest="subparser_name"
    )

    subparsers = {}
    for source in arg_sources:
        name, sub = source(subcommands)
        subparsers[name] = sub

    parsed_args = parser.parse_args()

    # call a subcommand-specific arg-checker
    valid, name = checkers[parsed_args.subparser_name](parsed_args)
    if not valid:
        print("invalid")
        subparsers[name].print_help()
        return None

    return parsed_args

__version__ = "0.1.0"
