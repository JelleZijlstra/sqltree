from dataclasses import dataclass
import enum
from typing import Optional, Set, Tuple

Version = Optional[Tuple[int, ...]]


class Vendor(enum.Enum):
    mysql = 1
    presto = 2
    redshift = 3


@dataclass
class Dialect:
    vendor: Vendor
    # If omitted, we assume the most recent version
    version: Version = None
    _keywords: Optional[Set[str]] = None

    def get_keywords(self) -> Set[str]:
        if self._keywords is not None:
            return self._keywords
        keywords = _compute_keywords(self.vendor, self.version)
        self._keywords = keywords
        return keywords


DEFAULT_DIALECT = Dialect(Vendor.mysql)


def version_is_in(
    version: Version, *, start_version: Version = None, end_version: Version = None
) -> bool:
    """Is this version within this range?

    For example, if a feature was added in version 1.1 and removed in 1.3, you would query:

        version_is_in(version, start_version=(1, 1), end_version=(1, 3))

    """
    if version is None:
        # If we haven't specified a version, we assume the most recent version, so anything
        # without an end_version matches.
        return end_version is None
    if start_version is not None and version < start_version:
        return False
    if end_version is not None and version >= end_version:
        return False
    return True


# from https://dev.mysql.com/doc/refman/5.7/en/keywords.html#keywords-in-current-series
BASE_MYSQL_KEYWORDS = {
    "ACCESSIBLE",
    "ADD",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "AS",
    "ASC",
    "ASENSITIVE",
    "BEFORE",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BLOB",
    "BOTH",
    "BY",
    "CALL",
    "CASCADE",
    "CASE",
    "CHANGE",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONDITION",
    "CONSTRAINT",
    "CONTINUE",
    "CONVERT",
    "CREATE",
    "CROSS",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURSOR",
    "DATABASE",
    "DATABASES",
    "DAY_HOUR",
    "DAY_MICROSECOND",
    "DAY_MINUTE",
    "DAY_SECOND",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DELAYED",
    "DELETE",
    "DESC",
    "DESCRIBE",
    "DETERMINISTIC",
    "DISTINCT",
    "DISTINCTROW",
    "DIV",
    "DOUBLE",
    "DROP",
    "DUAL",
    "EACH",
    "ELSE",
    "ELSEIF",
    "ENCLOSED",
    "ESCAPED",
    "EXISTS",
    "EXIT",
    "EXPLAIN",
    "FALSE",
    "FETCH",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "FOR",
    "FORCE",
    "FOREIGN",
    "FROM",
    "FULLTEXT",
    "GENERATED",
    "GET",
    "GRANT",
    "GROUP",
    "HAVING",
    "HIGH_PRIORITY",
    "HOUR_MICROSECOND",
    "HOUR_MINUTE",
    "HOUR_SECOND",
    "IF",
    "IGNORE",
    "IN",
    "INDEX",
    "INFILE",
    "INNER",
    "INOUT",
    "INSENSITIVE",
    "INSERT",
    "INT",
    "INT1",
    "INT2",
    "INT3",
    "INT4",
    "INT8",
    "INTEGER",
    "INTERVAL",
    "INTO",
    "IO_AFTER_GTIDS",
    "IO_BEFORE_GTIDS",
    "IS",
    "ITERATE",
    "JOIN",
    "KEY",
    "KEYS",
    "KILL",
    "LEADING",
    "LEAVE",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LINEAR",
    "LINES",
    "LOAD",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCK",
    "LONG",
    "LONGBLOB",
    "LONGTEXT",
    "LOOP",
    "LOW_PRIORITY",
    "MASTER_BIND",
    "MASTER_SSL_VERIFY_SERVER_CERT",
    "MATCH",
    "MAXVALUE",
    "MEDIUMBLOB",
    "MEDIUMINT",
    "MEDIUMTEXT",
    "MIDDLEINT",
    "MINUTE_MICROSECOND",
    "MINUTE_SECOND",
    "MOD",
    "MODIFIES",
    "NATURAL",
    "NOT",
    "NO_WRITE_TO_BINLOG",
    "NULL",
    "NUMERIC",
    "ON",
    "OPTIMIZE",
    "OPTIMIZER_COSTS",
    "OPTION",
    "OPTIONALLY",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OUTFILE",
    "PARTITION",
    "PRECISION",
    "PRIMARY",
    "PROCEDURE",
    "PURGE",
    "RANGE",
    "READ",
    "READS",
    "READ_WRITE",
    "REAL",
    "REFERENCES",
    "REGEXP",
    "RELEASE",
    "RENAME",
    "REPEAT",
    "REPLACE",
    "REQUIRE",
    "RESIGNAL",
    "RESTRICT",
    "RETURN",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "SCHEMA",
    "SCHEMAS",
    "SECOND_MICROSECOND",
    "SELECT",
    "SENSITIVE",
    "SEPARATOR",
    "SET",
    "SHOW",
    "SIGNAL",
    "SMALLINT",
    "SPATIAL",
    "SPECIFIC",
    "SQL",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQL_BIG_RESULT",
    "SQL_CALC_FOUND_ROWS",
    "SQL_SMALL_RESULT",
    "SSL",
    "STARTING",
    "STORED",
    "STRAIGHT_JOIN",
    "TABLE",
    "TERMINATED",
    "THEN",
    "TINYBLOB",
    "TINYINT",
    "TINYTEXT",
    "TO",
    "TRAILING",
    "TRIGGER",
    "TRUE",
    "UNDO",
    "UNION",
    "UNIQUE",
    "UNLOCK",
    "UNSIGNED",
    "UPDATE",
    "USAGE",
    "USE",
    "USING",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "VALUES",
    "VARBINARY",
    "VARCHAR",
    "VARCHARACTER",
    "VARYING",
    "VIRTUAL",
    "WHEN",
    "WHERE",
    "WHILE",
    "WITH",
    "WRITE",
    "XOR",
    "YEAR_MONTH",
    "ZEROFILL",
}
MYSQL8_NEW_KEYWORDS = {
    "CUME_DIST",
    "DENSE_RANK",
    "EMPTY",
    "EXCEPT",
    "FIRST_VALUE",
    "GROUPING",
    "GROUPS",
    "JSON_TABLE",
    "LAG",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "NTH_VALUE",
    "NTILE",
    "OF",
    "OVER",
    "PERCENT_RANK",
    "RANK",
    "RECURSIVE",
    "ROW_NUMBER",
    "SYSTEM",
    "WINDOW",
}


def _compute_keywords(vendor: Vendor, version: Version) -> Set[str]:
    if vendor is Vendor.mysql:
        keywords = BASE_MYSQL_KEYWORDS
        if version_is_in(version, start_version=(8,)):
            return keywords | MYSQL8_NEW_KEYWORDS
        else:
            return keywords
    else:
        raise NotImplementedError(vendor)
