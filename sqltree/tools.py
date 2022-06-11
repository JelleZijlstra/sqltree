"""

Tools for working with SQL.

"""


from typing import Sequence

from .dialect import DEFAULT_DIALECT, Dialect
from .parser import Dotted, Identifier, IntoClause, SimpleTableFactor
from .api import sqltree
from .visitor import walk


def get_tables(sql: str, dialect: Dialect = DEFAULT_DIALECT) -> Sequence[str]:
    """Find all tables referenced by a SQL statement."""
    tree = sqltree(sql, dialect=dialect)

    tables = []
    for node in walk(tree):
        if isinstance(node, SimpleTableFactor):
            if isinstance(node.table_name, Identifier):
                tables.append(node.table_name.text)
            else:
                tables.append(
                    f"{node.table_name.left.text}.{node.table_name.right.text}"
                )
        elif isinstance(node, IntoClause):
            if isinstance(node.table, Identifier):
                tables.append(node.table.text)
            elif isinstance(node.table, Dotted):
                tables.append(f"{node.table.left.text}.{node.table.right.text}")
    return tables
