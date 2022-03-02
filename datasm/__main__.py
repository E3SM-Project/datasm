import sys

from datasm import parse_args
from datasm.warehouse import AutoWarehouse
from datasm.workflows.extraction import Extraction
from datasm.workflows.cleanup import CleanUp
from datasm.workflows.postprocess import PostProcess
from datasm.workflows.publication import Publication
from datasm.workflows.validation import Validation

subcommands = {
    "auto": AutoWarehouse,
    "extract": Extraction,
    "cleanup": CleanUp,
    "postprocess": PostProcess,
    "publish": Publication,
    "validate": Validation,
}
arg_sources = [
    AutoWarehouse.add_args,
    Publication.add_args,
    Validation.add_args,
    Extraction.add_args,
    CleanUp.add_args,
    PostProcess.add_args,
]
arg_checkers = {
    "auto": AutoWarehouse.arg_checker,
    "extract": Extraction.arg_checker,
    "cleanup": CleanUp.arg_checker,
    "postprocess": PostProcess.arg_checker,
    "publish": Publication.arg_checker,
    "validate": Validation.arg_checker,
}


def main():
    args = parse_args(arg_sources, arg_checkers)
    if not args:
        return -1
    command = args.subparser_name
    job = subcommands[command](**vars(args))
    return job()


if __name__ == "__main__":
    sys.exit(main())
