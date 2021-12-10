from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, Set
from .dialect import Dialect
from .peeking_iterator import PeekingIterator
import enum


class TokenizeError(Exception):
    pass


class TokenType(enum.Enum):
    keyword = 1
    punctuation = 2
    string = 3
    number = 4

    def make(self, text: str) -> "Token":
        return Token(self, text)


@dataclass
class Token:
    typ: TokenType
    text: str


PUNCTUATION = {
    ".",
    "(",
    ")",
    ",",
    "+",
    "*",
    ">=",
    "<=",
    "=",
    "<>",
    "!=",
    "/",
    "%",
    "-",
    "~",
    "&",
    "^",
    "|",
}
QUOTATIONS = {"`", "'", '"'}


def tokenize(sql: str, dialect: Dialect) -> Iterable[Token]:
    # Prepare punctuation
    starting_char_to_continuations: Dict[str, Set[str]] = defaultdict(set)
    for punc in PUNCTUATION:
        if len(punc) == 1:
            starting_char_to_continuations[punc].add("")
        elif len(punc) == 2:
            starting_char_to_continuations[punc[0]].add(punc[1])
        else:
            raise TokenizeError(f"don't know how to handle {punc}")

    pi = PeekingIterator(sql)
    while pi.has_next():
        char = pi.next()
        if char.isalpha():
            pi.wind_back()
            text = _consume_identifier(pi)
            yield Token(TokenType.keyword, text)
        elif char.isspace():
            continue  # Skip over whitespace
        elif char in starting_char_to_continuations:
            continuations = starting_char_to_continuations[char]
            if not pi.has_next():
                if "" in continuations:
                    yield Token(TokenType.punctuation, char)
                else:
                    raise TokenizeError(
                        f"unexpected EOF following {char} (expected one of {continuations})"
                    )
            else:
                c = pi.next()
                if c in continuations:
                    yield Token(TokenType.punctuation, char + c)
                elif "" in c:
                    yield Token(TokenType.punctuation, char)
                else:
                    raise TokenizeError(
                        f"unexpected {c} following {char} (expected one of {continuations})"
                    )
        elif char.isnumeric():
            pi.wind_back()
            yield Token(TokenType.number, _consume_integer(pi))
        elif char in QUOTATIONS:
            yield Token(TokenType.string, _consume_until(pi, char))


def _consume_until(pi: PeekingIterator[str], end: str) -> str:
    chars = [end]
    for c in pi:
        chars.append(c)
        if c == end:
            return "".join(chars)
    raise TokenizeError(f"unexpected EOF (expected {end})")


def _consume_identifier(pi: PeekingIterator[str]) -> str:
    chars = []
    for c in pi:
        if c.isalnum() or c == "_":
            chars.append(c)
        else:
            pi.wind_back()
            return "".join(chars)
    return "".join(chars)


def _consume_integer(pi: PeekingIterator[str]) -> str:
    chars = []
    for c in pi:
        if c.isnumeric():
            chars.append(c)
        else:
            pi.wind_back()
            return "".join(chars)
    return "".join(chars)
