from functools import partial
from typing import Sequence

from sqltree.dialect import Dialect
from sqltree.location import Location
from sqltree.tokenizer import Token, TokenType, tokenize


def check(sql: str, tokens: Sequence[Token]) -> None:
    actual = list(tokenize(sql, Dialect.mysql))
    assert actual == tokens


def test_tokenize() -> None:
    sql = "SELECT * FROM table WHERE x = 3 AND y = 'x' AND z = {x} AND alpha = %s -- x"
    L = partial(Location, sql)
    check(
        sql,
        [
            TokenType.identifier.make("SELECT", L(0, 5)),
            TokenType.punctuation.make("*", L(7, 7)),
            TokenType.identifier.make("FROM", L(9, 12)),
            TokenType.identifier.make("table", L(14, 18)),
            TokenType.identifier.make("WHERE", L(20, 24)),
            TokenType.identifier.make("x", L(26, 26)),
            TokenType.punctuation.make("=", L(28, 28)),
            TokenType.number.make("3", L(30, 30)),
            TokenType.identifier.make("AND", L(32, 34)),
            TokenType.identifier.make("y", L(36, 36)),
            TokenType.punctuation.make("=", L(38, 38)),
            TokenType.string.make("'x'", L(40, 42)),
            TokenType.identifier.make("AND", L(44, 46)),
            TokenType.identifier.make("z", L(48, 48)),
            TokenType.punctuation.make("=", L(50, 50)),
            TokenType.placeholder.make("{x}", L(52, 54)),
            TokenType.identifier.make("AND", L(56, 58)),
            TokenType.identifier.make("alpha", L(60, 64)),
            TokenType.punctuation.make("=", L(66, 66)),
            TokenType.placeholder.make("%s", L(68, 69)),
            TokenType.comment.make("-- x", L(71, 74)),
        ],
    )


def test_comment() -> None:
    sql = "SELECT /* c */ * FROM table # x"
    L = partial(Location, sql)
    check(
        sql,
        [
            TokenType.identifier.make("SELECT", L(0, 5)),
            TokenType.comment.make("/* c */", L(7, 13)),
            TokenType.punctuation.make("*", L(15, 15)),
            TokenType.identifier.make("FROM", L(17, 20)),
            TokenType.identifier.make("table", L(22, 26)),
            TokenType.comment.make("# x", L(28, 30)),
        ],
    )
