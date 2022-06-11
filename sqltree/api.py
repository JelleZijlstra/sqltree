from .dialect import DEFAULT_DIALECT, Dialect
from .keywords import distinguish_keywords
from .mangler import mangle
from .parser import Statement, parse
from .tokenizer import tokenize


def sqltree(sql: str, dialect: Dialect = DEFAULT_DIALECT) -> Statement:
    tokens = tokenize(sql, dialect)
    tokens = distinguish_keywords(tokens, dialect)
    tokens = mangle(tokens)
    return parse(tokens, dialect)
