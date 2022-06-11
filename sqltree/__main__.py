import argparse

from .api import sqltree
from .dialect import DEFAULT_DIALECT, Dialect, Vendor, Version
from .parser import ParseError


def parse_version(version: str) -> Version:
    pieces = version.split(".")
    try:
        return tuple(int(piece) for piece in pieces)
    except ValueError:
        raise argparse.ArgumentError(None, f"Invalid version {version!r}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("sqltree")
    parser.add_argument("sql", help="SQL string to parse")
    parser.add_argument("--dialect", choices=Vendor, type=Vendor.__getitem__)
    parser.add_argument("--version", type=parse_version)
    args = parser.parse_args()
    if args.dialect is not None:
        dialect = Dialect(args.dialect, args.version)
    else:
        dialect = DEFAULT_DIALECT
    try:
        tree = sqltree(args.sql, dialect)
    except ParseError as e:
        print(e)
    else:
        print(tree)
