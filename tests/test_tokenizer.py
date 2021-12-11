from typing import Sequence
from sqltree.dialect import Dialect
from sqltree.tokenizer import tokenize, Token, TokenType


def check(sql: str, tokens: Sequence[Token]) -> None:
    actual = list(tokenize(sql, Dialect.mysql))
    assert actual == tokens


def test_tokenize() -> None:
    check(
        "SELECT * FROM table WHERE x = 3 AND y = 'x'",
        [
            TokenType.identifier.make("SELECT"),
            TokenType.punctuation.make("*"),
            TokenType.identifier.make("FROM"),
            TokenType.identifier.make("table"),
            TokenType.identifier.make("WHERE"),
            TokenType.identifier.make("x"),
            TokenType.punctuation.make("="),
            TokenType.number.make("3"),
            TokenType.identifier.make("AND"),
            TokenType.identifier.make("y"),
            TokenType.punctuation.make("="),
            TokenType.identifier.make("'x'"),
        ],
    )
