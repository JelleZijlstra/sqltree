from sqltree import parser as p
from sqltree.location import Location
from sqltree.sqltree import sqltree
from sqltree.tokenizer import Token, TokenType

T = Token(TokenType.keyword, "x", Location("x", 1, 1))


def test() -> None:
    tree = sqltree("SELECT * FROM a")
    assert tree == p.Select(
        p.Keyword(T, "SELECT"),
        [p.SelectExpr(p.Star(T), None, None, None)],
        p.Keyword(T, "FROM"),
        p.Identifier(T, "a"),
        None,
        None,
    )

    tree = sqltree(
        "SELECT a, b AS c FROM a WHERE x = 3 * 3 + 2 * 4 AND y = 'x' AND z = {x}"
    )
    assert tree == p.Select(
        p.Keyword(T, "SELECT"),
        [
            p.SelectExpr(p.Identifier(T, "a"), None, None, p.Punctuation(T, ",")),
            p.SelectExpr(
                p.Identifier(T, "b"), p.Keyword(T, "AS"), p.Identifier(T, "c"), None
            ),
        ],
        p.Keyword(T, "FROM"),
        p.Identifier(T, "a"),
        p.Keyword(T, "WHERE"),
        p.BinOp(
            p.BinOp(
                p.BinOp(
                    p.Identifier(T, "x"),
                    p.Punctuation(T, "="),
                    p.BinOp(
                        p.BinOp(
                            p.IntegerLiteral(T, 3),
                            p.Punctuation(T, "*"),
                            p.IntegerLiteral(T, 3),
                        ),
                        p.Punctuation(T, "+"),
                        p.BinOp(
                            p.IntegerLiteral(T, 2),
                            p.Punctuation(T, "*"),
                            p.IntegerLiteral(T, 4),
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
                p.Identifier(T, "z"), p.Punctuation(T, "="), p.Placeholder(T, "{x}")
            ),
        ),
    )
