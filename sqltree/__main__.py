import argparse

from .parser import ParseError
from .sqltree import sqltree

if __name__ == "__main__":
    parser = argparse.ArgumentParser("sqltree")
    parser.add_argument("sql", help="SQL string to parse")
    args = parser.parse_args()
    try:
        tree = sqltree(args.sql)
    except ParseError as e:
        print(e)
    else:
        print(tree)
