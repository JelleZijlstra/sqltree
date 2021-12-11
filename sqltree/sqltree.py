from .keywords import distinguish_keywords
from .dialect import Dialect
from .tokenizer import tokenize
from .parser import Statement, parse


def sqltree(sql: str, dialect: Dialect = Dialect.mysql) -> Statement:
    tokens = tokenize(sql, dialect)
    tokens = distinguish_keywords(tokens, dialect)
    return parse(tokens)
