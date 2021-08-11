import argparse
from argparse import RawTextHelpFormatter


def parse_args(arg_sources, checkers):
    DESC = "Automated E3SM data warehouse utilities"
    parser = argparse.ArgumentParser(
        prog="warehouse",
        description=DESC,
        prefix_chars="-",
        formatter_class=RawTextHelpFormatter,
    )

    subcommands = parser.add_subparsers(
        title="subcommands", description="warehouse subcommands", dest="subparser_name"
    )

    subparsers = {}
    for source in arg_sources:
        name, sub = source(subcommands)
        subparsers[name] = sub

    parsed_args = parser.parse_args()

    valid, name = checkers[parsed_args.subparser_name](parsed_args)
    if not valid:
        print("invalid")
        subparsers[name].print_help()
        return None

    return parsed_args
