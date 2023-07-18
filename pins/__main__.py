"""Command line interface for managing pins."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pins


BOARD_PATH = Path(pins.config.get_config_dir()) / "cli" / "current_board.json"


def pin_list(args):
    board = pin_get_board()
    pattern = re.compile(args.pattern)
    for pin in filter(pattern.search, board.pin_list()):
        print(pin)


def pin_meta(args):
    board = pin_get_board()
    meta = board.pin_meta(args.pin)

    if args.format == "json":
        data = json.dumps(meta.to_pin_dict(), indent=2, sort_keys=True)
    else:
        assert args.format == "yaml"
        data = meta.to_pin_yaml().rstrip()

    print(data)


def pin_write(args):
    board = pin_get_board()

    data = args.data
    type = args.type

    if type == "file":
        data = args.data
    elif type == "csv":
        import pyarrow.csv as pac

        data = pac.read_csv(data).to_pandas()

    elif type == "parquet":
        import pyarrow.parquet as pq

        data = pq.read_table(data).to_pandas()
    elif type == "json":
        import pyarrow.json as pj

        data = pj.read_json(data).to_pandas()
    else:
        raise NotImplementedError(f"Unsupported file type: {type}")

    board.pin_write(
        data, name=args.pin, type=type, title=args.title, description=args.description
    )


def pin_set_board(args):
    BOARD_PATH.parent.mkdir(parents=True, exist_ok=True)
    BOARD_PATH.write_text(json.dumps({"path": args.board, "protocol": args.protocol}))


def pin_get_board():
    config = json.loads(BOARD_PATH.read_text())
    return pins.board(**config)


def main(parser) -> None:
    args = parser.parse_args()

    try:
        args.func(args)
    except Exception as e:
        parser.error(str(e))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="pins",
        description="Manage pins from the command line.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    subparsers = parser.add_subparsers(required=True)

    pin_set_board_parser = subparsers.add_parser(
        "set-board",
        description="Set the board to use for subcommands.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    pin_set_board_parser.add_argument("board", help="The name of the board")
    pin_set_board_parser.add_argument(
        "-p",
        "--protocol",
        required=True,
        help="The protocol of the board: gcs, s3, etc.",
    )
    pin_set_board_parser.set_defaults(func=pin_set_board)

    pin_list_parser = subparsers.add_parser(
        "list",
        description="List all pins.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    pin_list_parser.add_argument(
        "-p", "--pattern", help="Pattern to match pins against", default=".*"
    )
    pin_list_parser.set_defaults(func=pin_list)

    pin_meta_parser = subparsers.add_parser(
        "meta",
        description="Get metadata for a pin.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    pin_meta_parser.add_argument("pin", help="The name of the pin")
    pin_meta_parser.add_argument(
        "-f",
        "--format",
        choices=("json", "yaml"),
        default="json",
        help="Pin metadata output format",
    )
    pin_meta_parser.set_defaults(func=pin_meta)

    pin_write_parser = subparsers.add_parser(
        "write",
        description="Write data to a pin.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    pin_write_parser.add_argument("pin", help="The name of the pin")
    pin_write_parser.add_argument("data", help="Path to the data to write")
    pin_write_parser.add_argument(
        "-t",
        "--type",
        choices=("file", "csv", "parquet", "json"),
        help="Pin data format",
        default="file",
    )
    pin_write_parser.add_argument("-T", "--title", help="Pin title", default=None)
    pin_write_parser.add_argument(
        "-d", "--description", help="Pin description", default=None
    )

    main(parser)
