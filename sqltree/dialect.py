import enum
from dataclasses import dataclass, field
from typing import Dict, Optional, Sequence, Set, Tuple, Union

Version = Optional[Tuple[int, ...]]


class Vendor(enum.Enum):
    mysql = 1
    presto = 2
    redshift = 3


class Feature(enum.Enum):
    require_into_for_ignore = 1  # if False, allow omitting INTO in INSERT INTO
    support_value_for_insert = 2  # support using VALUE instead of VALUES in INSERT
    insert_ignore = 3  # INSERT IGNORE
    default_values_on_insert = 4  # INSERT ... DEFAULT VALUES
    insert_select_require_parens = (
        5  # INSERT ... SELECT requires parentheses around the SELECT
    )
    replace = 6  # REPLACE statement
    with_clause = 7  # leading WITH clause in DELETE, UPDATE, SELECT
    require_from_for_delete = 8  # if False, allow omittinng FROM in DELETE FROM
    update_limit = 9  # ORDER BY and LIMIT on DELETE and UPDATE
    delete_using = 10  # USING in DELETE
    comma_offset = 11  # LIMIT offset, row_count
    limit_all = 12  # LIMIT ALL


@dataclass
class Dialect:
    vendor: Vendor
    # If omitted, we assume the most recent version
    version: Version = None
    _keywords: Optional[Set[str]] = field(
        compare=False, repr=False, hash=False, init=False, default=None
    )

    def __str__(self) -> str:
        name = self.vendor.name
        if self.version is not None:
            name += " " + ".".join(map(str, self.version))
        return name

    def get_keywords(self) -> Set[str]:
        if self._keywords is not None:
            return self._keywords
        keywords = _compute_keywords(self.vendor, self.version)
        self._keywords = keywords
        return keywords

    def supports_feature(self, feature: Feature) -> bool:
        value = _FEATURES[feature].get(self.vendor, True)
        if isinstance(value, bool):
            return value
        start_version, end_version = value
        return version_is_in(
            self.version, start_version=start_version, end_version=end_version
        )

    def get_identifier_delimiter(self) -> str:
        return _IDENTIFIER_QUOTE[self.vendor]

    def get_select_modifiers(self) -> Sequence[Tuple[str, ...]]:
        if self.vendor is Vendor.mysql:
            return [
                ("ALL", "DISTINCT", "DISTINCTROW"),
                ("HIGH_PRIORITY",),
                ("STRAIGHT_JOIN",),
                ("SQL_SMALL_RESULT",),
                ("SQL_BIG_RESULT",),
                ("SQL_BUFFER_RESULT",),
                ("SQL_NO_CACHE",)
                if version_is_in(self.version, start_version=(8,))
                else ("SQL_CACHE", "SQL_NO_CACHE"),
                ("SQL_CALC_FOUND_ROWS",),
            ]
        elif self.vendor is Vendor.redshift or self.vendor is Vendor.presto:
            return [("ALL", "DISTINCT")]
        else:
            raise NotImplementedError(self.vendor)


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


# Values can be either a boolean (indicating support across all versions) or a version range
_FEATURES: Dict[Feature, Dict[Vendor, Union[bool, Tuple[Version, Version]]]] = {
    Feature.require_into_for_ignore: {Vendor.mysql: False, Vendor.redshift: True},
    Feature.support_value_for_insert: {Vendor.mysql: True, Vendor.redshift: False},
    Feature.insert_ignore: {Vendor.mysql: True, Vendor.redshift: False},
    Feature.default_values_on_insert: {Vendor.mysql: False, Vendor.redshift: True},
    Feature.insert_select_require_parens: {Vendor.mysql: False, Vendor.redshift: True},
    Feature.replace: {Vendor.mysql: True, Vendor.redshift: False},
    Feature.with_clause: {
        Vendor.mysql: False,
        Vendor.presto: True,
        Vendor.redshift: True,
    },
    Feature.require_from_for_delete: {Vendor.mysql: True, Vendor.redshift: False},
    Feature.update_limit: {Vendor.mysql: True, Vendor.redshift: False},
    Feature.delete_using: {Vendor.mysql: False, Vendor.redshift: True},
    Feature.comma_offset: {Vendor.mysql: True, Vendor.redshift: False},
    Feature.limit_all: {Vendor.mysql: False, Vendor.redshift: True},
}
_missing_features = set(Feature) - set(_FEATURES)
assert not _missing_features, f"missing settings for {_missing_features}"

_IDENTIFIER_QUOTE = {
    Vendor.mysql: "`",
    Vendor.presto: '"',
    Vendor.redshift: '"',  # https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
}


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
# https://prestodb.io/docs/current/language/reserved.html
PRESTO_KEYWORDS = {
    "SET",  # not actually a keyword but otherwise we can't parse UPDATE
    "ALTER",
    "AND",
    "AS",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEALLOCATE",
    "DELETE",
    "DESCRIBE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXECUTE",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IN",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "LEFT",
    "LIKE",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NATURAL",
    "NORMALIZE",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PREPARE",
    "RECURSIVE",
    "RIGHT",
    "ROLLUP",
    "SELECT",
    "TABLE",
    "THEN",
    "TRUE",
    "UESCAPE",
    "UNION",
    "UNNEST",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    "WITH",
}
# https://docs.aws.amazon.com/redshift/latest/dg/r_pg_keywords.html
REDSHIFT_KEYWORDS = {
    "SET",  # not actually a keyword but otherwise we can't parse UPDATE
    "AES128",
    "AES256",
    "ALL",
    "ALLOWOVERWRITE",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "AUTHORIZATION",
    "AZ64",
    "BACKUP",
    "BETWEEN",
    "BINARY",
    "BLANKSASNULL",
    "BOTH",
    "BYTEDICT",
    "BZIP2",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONSTRAINT",
    "CREATE",
    "CREDENTIALS",
    "CROSS",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURRENT_USER_ID",
    "DEFAULT",
    "DEFERRABLE",
    "DEFLATE",
    "DEFRAG",
    "DELTA",
    "DELTA32K",
    "DESC",
    "DISABLE",
    "DISTINCT",
    "DO",
    "ELSE",
    "EMPTYASNULL",
    "ENABLE",
    "ENCODE",
    "ENCRYPT",
    "ENCRYPTION",
    "END",
    "EXCEPT",
    "EXPLICIT",
    "FALSE",
    "FOR",
    "FOREIGN",
    "FREEZE",
    "FROM",
    "FULL",
    "GLOBALDICT256",
    "GLOBALDICT64K",
    "GRANT",
    "GROUP",
    "GZIP",
    "HAVING",
    "IDENTITY",
    "IGNORE",
    "ILIKE",
    "IN",
    "INITIALLY",
    "INNER",
    "INTERSECT",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "LANGUAGE",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LUN",
    "LUNS",
    "LZO",
    "LZOP",
    "MINUS",
    "MOSTLY16",
    "MOSTLY32",
    "MOSTLY8",
    "NATURAL",
    "NEW",
    "NOT",
    "NOTNULL",
    "NULL",
    "NULLS",
    "OFF",
    "OFFLINE",
    "OFFSET",
    "OID",
    "OLD",
    "ON",
    "ONLY",
    "OPEN",
    "OR",
    "ORDER",
    "OUTER",
    "OVERLAPS",
    "PARALLEL",
    "PARTITION",
    "PERCENT",
    "PERMISSIONS",
    "PLACING",
    "PRIMARY",
    "RAW",
    "READRATIO",
    "RECOVER",
    "REFERENCES",
    "RESPECT",
    "REJECTLOG",
    "RESORT",
    "RESTORE",
    "RIGHT",
    "SELECT",
    "SESSION_USER",
    "SIMILAR",
    "SNAPSHOT ",
    "SOME",
    "SYSDATE",
    "SYSTEM",
    "TABLE",
    "TAG",
    "TDES",
    "TEXT255",
    "TEXT32K",
    "THEN",
    "TIMESTAMP",
    "TO",
    "TOP",
    "TRAILING",
    "TRUE",
    "TRUNCATECOLUMNS",
    "UNION",
    "UNIQUE",
    "USER",
    "USING",
    "VERBOSE",
    "WALLET",
    "WHEN",
    "WHERE",
    "WITH",
    "WITHOUT",
}


def _compute_keywords(vendor: Vendor, version: Version) -> Set[str]:
    if vendor is Vendor.mysql:
        keywords = BASE_MYSQL_KEYWORDS
        if version_is_in(version, start_version=(8,)):
            return keywords | MYSQL8_NEW_KEYWORDS
        else:
            return keywords
    elif vendor is Vendor.presto:
        return PRESTO_KEYWORDS
    elif vendor is Vendor.redshift:
        return REDSHIFT_KEYWORDS
    else:
        raise NotImplementedError(vendor)
