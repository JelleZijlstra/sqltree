from typing import Sequence
from sqlparse.dialect import Dialect
from sqlparse.tokenizer import tokenize, Token, TokenType


def check(sql: str, tokens: Sequence[Token]) -> None:
    actual = list(tokenize(sql, Dialect.mysql))
    assert actual == tokens


def test_tokenize() -> None:
    check(
        "SELECT * FROM table WHERE x = 3 AND y = 'x'",
        [
            TokenType.keyword.make("SELECT"),
            TokenType.punctuation.make("*"),
            TokenType.keyword.make("FROM"),
            TokenType.keyword.make("table"),
            TokenType.keyword.make("WHERE"),
            TokenType.keyword.make("x"),
            TokenType.punctuation.make("="),
            TokenType.number.make("3"),
            TokenType.keyword.make("AND"),
            TokenType.keyword.make("y"),
            TokenType.punctuation.make("="),
            TokenType.string.make("'x'"),
        ],
    )
