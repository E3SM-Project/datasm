import sys

from warehouse.report import report, add_report_args, check_report_args
from warehouse import parse_args

from warehouse.workflows.extraction import Extraction
from warehouse.workflows.cleanup import CleanUp
from warehouse.workflows.postprocess import PostProcess
from warehouse.workflows.publication import Publication
from warehouse.workflows.validation import Validation

subcommands = {
    'report': report,
    'extract': Extraction,
    'cleanup': CleanUp,
    'postprocess': PostProcess,
    'publication': Publication,
    'validation': Validation
}
arg_sources = [
    add_report_args,
    Extraction.add_args,
    CleanUp.add_args,
    PostProcess.add_args,
    Publication.add_args,
    Validation.add_args
]
arg_checkers = [
    check_report_args,
    Extraction.arg_checker,
    CleanUp.arg_checker,
    PostProcess.arg_checker,
    Publication.arg_checker,
    Validation.arg_checker
]


def main():
    args = parse_args(arg_sources, arg_checkers)
    if not args:
        return -1
    command = args.subparser_name
    subcommands[command](args)


if __name__ == "__main__":
    sys.exit(main())
