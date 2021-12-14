from typing import Iterable

from .dialect import Dialect
from .tokenizer import Token, TokenType


def distinguish_keywords(tokens: Iterable[Token], dialect: Dialect) -> Iterable[Token]:
    keywords = dialect.get_keywords()
    for token in tokens:
        if token.typ is TokenType.identifier:
            text = token.text.upper()
            if text in keywords:
                yield TokenType.keyword.make(text, token.loc)
            else:
                yield token
        else:
            yield token
