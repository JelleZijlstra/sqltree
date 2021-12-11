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
    identifier = 5
    placeholder = 6

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
            yield Token(TokenType.identifier, text)
        elif char.isspace():
            continue  # Skip over whitespace
        elif char == "%":
            next_char = pi.peek()
            if next_char is not None and next_char.isalpha():
                rest = _consume_identifier(pi)
                yield Token(TokenType.placeholder, "%" + rest)
            else:
                yield Token(TokenType.punctuation, "%")
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
            # TODO floats, hex, other kinds of numbers?
            pi.wind_back()
            yield Token(TokenType.number, _consume_integer(pi))
        elif char in QUOTATIONS:
            yield Token(TokenType.string, char + _consume_until(pi, char))
        elif char == "{":
            yield Token(TokenType.placeholder, "{" + _consume_until(pi, "}"))
        else:
            # TODO comments
            raise TokenizeError(f"unexpected character {char}")


def _consume_until(pi: PeekingIterator[str], end: str) -> str:
    chars = []
    for c in pi:
        chars.append(c)
        # TODO backslash escapes?
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
