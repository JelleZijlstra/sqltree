# sqltree

`sqltree` is an experimental parser for SQL, providing
a syntax tree for SQL queries. Possible use cases include:

- Static analysis (for example, to validate column names)
- Translating queries to another SQL dialect
- Autoformatting

`sqltree` can parse queries:

```
$ python -m sqltree "SELECT * FROM x WHERE x = 3"
Select(select_exprs=[SelectExpr(expr=Star(), alias=None)], table=Identifier(text='x'), conditions=BinOp(left=Identifier(text='x'), op=Punctuation(text='='), right=IntegerLiteral(value=3)))
```

And format them:

```
$  python -m sqltree.formatter "SELECT * from x where x=3"
SELECT *
FROM x
WHERE x = 3
```

SQL is a big language with a complicated grammar that varies significantly
between database vendors. `sqltree` is designed to be flexible enough to parse
the full syntax supported by different databases, but I am prioritizing
constructs used in my use cases for the parser. So far, that has meant a focus
on parsing MySQL 8 queries. Further syntax will be added as I have time.

## Features

Useful features of `sqltree` include:

### Placeholder support

`sqltree` supports placeholders such as `%s` or `?` in various positions in
the query, so that queries using such placeholders can be formatted and analyzed.

```shell
$ python -m sqltree.formatter 'select * from x where y = 3 %(limit)s'
SELECT *
FROM x
WHERE y = 3
%(limit)s
```

### Better error messages

`sqltree`'s handwritten parser often produces better error messages than MySQL
itself. For example:

```shell
$ mysql
mysql> show replicca status;
ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'replicca status' at line 1
$ python -m sqltree 'show replicca status'
Unexpected 'replicca' (expected one of REPLICA, SLAVE, REPLICAS, TABLES, TABLE, TRIGGERS, VARIABLES, STATUS, COUNT, WARNINGS, ERRORS, COLUMNS, FIELDS, INDEX, INDEXES, KEYS)
0: show replicca status
        ^^^^^^^^
```

## API

- `sqltree.sqltree`: parse a SQL query and return the parse tree. See `sqltree.parser`
  for the possible parse nodes.
- `sqltree.formatter.format`: reformat a SQL query.
- `sqltree.tools.get_tables`: get the tables referenced in a SQL query.

More detailed documentation to follow.

## Requirements

`sqltree` runs on Python 3.6 and up and it has no dependencies.

## Using the fixit rule

sqltree embeds a [fixit](https://fixit.readthedocs.io/en/latest/) rule for
formatting SQL. Here is how to use it:

- Install fixit if you don't have it yet
  - `pip install fixit`
  - `python -m fixit.cli.init_config`
- Run `python -m fixit.cli.apply_fix --rules sqltree.fixit.SqlFormatRule path/to/your/code`

## Changelog

### Version 0.3.0 (July 12, 2022)

- Add ANSI SQL as a dialect
- Support escaping quotes by doubling them in string literals
- Support scientific notation with a negative exponent
- Fix formatting for quoted identifiers that contain non-alphanumeric characters
- Support the unary `NOT` operator
- Fix formatting of `LEFT JOIN` and similar queries

### Version 0.2.0 (June 24, 2022)

- Support `SELECT ... INTO` syntax
- Support `SET TRANSACTION` syntax
- Support `a MOD B` and `a DIV b` syntax
- Support `GROUP_CONCAT()` syntax

### Version 0.1.0 (June 13, 2022)

- Initial release
