from .dialect import Dialect
from .keywords import distinguish_keywords
from .mangler import mangle
from .parser import Statement, parse
from .tokenizer import tokenize


def sqltree(sql: str, dialect: Dialect = Dialect.mysql) -> Statement:
    tokens = tokenize(sql, dialect)
    tokens = distinguish_keywords(tokens, dialect)
    tokens = mangle(tokens)
    return parse(tokens)
