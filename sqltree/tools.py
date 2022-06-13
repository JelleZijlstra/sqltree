"""

Tools for working with SQL.

"""


from typing import Sequence

from .api import sqltree
from .dialect import DEFAULT_DIALECT, Dialect
from .parser import Dotted, DottedTable, Identifier, IntoClause, SimpleTableName
from .visitor import walk


def get_tables(sql: str, dialect: Dialect = DEFAULT_DIALECT) -> Sequence[str]:
    """Find all tables referenced by a SQL statement."""
    tree = sqltree(sql, dialect=dialect)

    tables = []
    for node in walk(tree):
        if isinstance(node, SimpleTableName):
            tables.append(node.identifier.text)
        elif isinstance(node, DottedTable):
            tables.append(f"{node.left.text}.{node.right.text}")
        elif isinstance(node, IntoClause):
            if isinstance(node.table, Identifier):
                tables.append(node.table.text)
            elif isinstance(node.table, Dotted):
                tables.append(f"{node.table.left.text}.{node.table.right.text}")
    return tables
