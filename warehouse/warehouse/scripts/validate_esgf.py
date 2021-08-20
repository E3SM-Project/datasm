import sys
import argparse
from pprint import pprint
from warehouse.dataset import Dataset, DatasetStatus


def parse_args():
    parser = argparse.ArgumentParser(description="Publish a dataset to ESGF")
    parser.add_argument(
        "--dataset-id",
        type=str,
        required=True,
        help="The dataset ID to use for the publication check",
    )
    return parser.parse_args()


def main():
    parsed_args = parse_args()
    dataset = Dataset(
        dataset_id=parsed_args.dataset_id,
        no_status_file=True)
    status = dataset.get_esgf_status()
    if status not in [DatasetStatus.SUCCESS.value, DatasetStatus.PUBLISHED.value]:
        print(f"ESGF validation failed, dataset in state {status}")
        if missing := dataset.missing:
            pprint(missing)
        return 1
    else:
        print("ESGF validation success")
        return 0


if __name__ == "__main__":
    sys.exit(main())
