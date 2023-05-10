from sqltree import parser as p
from sqltree.api import sqltree
from sqltree.dialect import Dialect, Vendor
from sqltree.location import Location
from sqltree.tokenizer import Token, TokenType

T = Token(TokenType.keyword, "x", Location("x", 1, 1))


def test() -> None:
    tree = sqltree("SELECT * FROM a")
    assert tree == p.Select(
        (),
        None,
        p.Keyword(T, "SELECT"),
        [],
        [p.WithTrailingComma(p.SelectExpr(p.Star(T), None, None))],
        None,
        p.FromClause(
            p.Keyword(T, "FROM"),
            [
                p.WithTrailingComma(
                    p.SimpleTableFactor(p.SimpleTableName(p.Identifier(T, "a")))
                )
            ],
        ),
    )

    tree = sqltree(
        "SELECT a, b AS c FROM a WHERE x = 3 * 3 + 2 * 4 AND y = 'x' AND z NOT IN {x}"
    )
    assert tree == p.Select(
        (),
        None,
        p.Keyword(T, "SELECT"),
        [],
        [
            p.WithTrailingComma(
                p.SelectExpr(p.Identifier(T, "a"), None, None), p.Punctuation(T, ",")
            ),
            p.WithTrailingComma(
                p.SelectExpr(
                    p.Identifier(T, "b"), p.Keyword(T, "AS"), p.Identifier(T, "c")
                )
            ),
        ],
        None,
        p.FromClause(
            p.Keyword(T, "FROM"),
            [
                p.WithTrailingComma(
                    p.SimpleTableFactor(p.SimpleTableName(p.Identifier(T, "a")))
                )
            ],
        ),
        p.WhereClause(
            p.Keyword(T, "WHERE"),
            p.BinOp(
                p.BinOp(
                    p.BinOp(
                        p.Identifier(T, "x"),
                        p.Punctuation(T, "="),
                        p.BinOp(
                            p.BinOp(
                                p.NumericLiteral(T, "3"),
                                p.Punctuation(T, "*"),
                                p.NumericLiteral(T, "3"),
                            ),
                            p.Punctuation(T, "+"),
                            p.BinOp(
                                p.NumericLiteral(T, "2"),
                                p.Punctuation(T, "*"),
                                p.NumericLiteral(T, "4"),
                            ),
                        ),
                    ),
                    p.Keyword(T, "AND"),
                    p.BinOp(
                        p.Identifier(T, "y"),
                        p.Punctuation(T, "="),
                        p.StringLiteral(T, "x"),
                    ),
                ),
                p.Keyword(T, "AND"),
                p.BinOp(
                    p.Identifier(T, "z"),
                    p.Keyword(T, "NOT IN"),
                    p.Placeholder(T, "{x}"),
                ),
            ),
        ),
    )

    tree = sqltree("-- comment\nSELECT * FROM a")
    assert tree == p.Select(
        (p.Comment(T, "-- comment\n"),),
        None,
        p.Keyword(T, "SELECT"),
        [],
        [p.WithTrailingComma(p.SelectExpr(p.Star(T), None, None))],
        None,
        p.FromClause(
            p.Keyword(T, "FROM"),
            [
                p.WithTrailingComma(
                    p.SimpleTableFactor(p.SimpleTableName(p.Identifier(T, "a")))
                )
            ],
        ),
    )

    tree = sqltree("SELECT a::char FROM a", dialect=Dialect(Vendor.redshift))
    assert tree == p.Select(
        (),
        None,
        p.Keyword(T, "SELECT"),
        [],
        [
            p.WithTrailingComma(
                p.SelectExpr(
                    p.ColonCast(
                        expr=p.Identifier(T, "a"),
                        double_colon=p.Punctuation(T, "::"),
                        type_name=p.CharType(p.Keyword(T, "char"), None),
                    ),
                    None,
                    None,
                )
            )
        ],
        None,
        p.FromClause(
            p.Keyword(T, "FROM"),
            [
                p.WithTrailingComma(
                    p.SimpleTableFactor(p.SimpleTableName(p.Identifier(T, "a")))
                )
            ],
        ),
    )

    tree = sqltree(
        """SELECT 
            (a)::char, 
            case when 1 then 1 else 2 end::char, 
            'a'::char 
        FROM a""",
        dialect=Dialect(Vendor.redshift),
    )
    assert tree == p.Select(
        (),
        None,
        p.Keyword(T, "SELECT"),
        [],
        [
            p.WithTrailingComma(
                p.SelectExpr(
                    p.ColonCast(
                        expr=p.Parenthesized(
                            p.Punctuation(T, "("),
                            p.Identifier(T, "a"),
                            p.Punctuation(T, ")"),
                        ),
                        double_colon=p.Punctuation(T, "::"),
                        type_name=p.CharType(p.Keyword(T, "char"), None),
                    ),
                    None,
                    None,
                ),
                p.Punctuation(T, ","),
            ),
            p.WithTrailingComma(
                p.SelectExpr(
                    p.ColonCast(
                        expr=p.CaseExpression(
                            p.Keyword(T, "CASE"),
                            None,
                            [
                                p.WhenThen(
                                    p.Keyword(T, "WHEN"),
                                    p.NumericLiteral(T, "1"),
                                    p.Keyword(T, "THEN"),
                                    p.NumericLiteral(T, "1"),
                                )
                            ],
                            p.ElseClause(
                                p.Keyword(T, "ELSE"), p.NumericLiteral(T, "2")
                            ),
                            p.Keyword(T, "END"),
                        ),
                        double_colon=p.Punctuation(T, "::"),
                        type_name=p.CharType(p.Keyword(T, "char"), None),
                    ),
                    None,
                    None,
                ),
                p.Punctuation(T, ","),
            ),
            p.WithTrailingComma(
                p.SelectExpr(
                    p.ColonCast(
                        expr=p.StringLiteral(T, "a"),
                        double_colon=p.Punctuation(T, "::"),
                        type_name=p.CharType(p.Keyword(T, "char"), None),
                    ),
                    None,
                    None,
                ),
                None,
            ),
        ],
        None,
        p.FromClause(
            p.Keyword(T, "FROM"),
            [
                p.WithTrailingComma(
                    p.SimpleTableFactor(p.SimpleTableName(p.Identifier(T, "a")))
                )
            ],
        ),
    )
