# sqltree

sqltree is an experimental parser for SQL, providing
a syntax tree for SQL queries. Possible use cases include:

- Static analysis (for example, to validate column names)
- Translating queries to another SQL dialect
- Autoformatting

sqltree is still in an early stage of development, but it
can already parse some queries:

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

## Using the fixit rule

sqltree embeds a [fixit](https://fixit.readthedocs.io/en/latest/) rule for
formatting SQL. Here is how to use it:

- Install fixit if you don't have it yet
  - `pip install fixit`
  - `python -m fixit.cli.init_config`
- Run `python -m fixit.cli.apply_fix --rules sqltree.fixit.SqlFormatRule path/to/your/code`
