import argparse

from .sqltree import sqltree

if __name__ == "__main__":
    parser = argparse.ArgumentParser("sqltree")
    parser.add_argument("sql", help="SQL string to parse")
    args = parser.parse_args()
    print(sqltree(args.sql))
