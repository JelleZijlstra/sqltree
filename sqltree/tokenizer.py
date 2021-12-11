import enum
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, Sequence, Set

from .dialect import Dialect
from .location import Location
from .peeking_iterator import PeekingIterator


class TokenizeError(Exception):
    pass


class TokenType(enum.Enum):
    keyword = 1
    punctuation = 2
    string = 3
    number = 4
    identifier = 5
    placeholder = 6
    comment = 7

    def make(self, text: str, loc: Location) -> "Token":
        return Token(self, text, loc)


@dataclass
class Token:
    typ: TokenType
    text: str
    loc: Location
    comments: Sequence["Token"] = ()


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
    ">>",
    "<<",
    "&&",
    "||",
    "--",  # Not a punctuation but a comment
    "/*",  # Also a comment
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
        start_index = pi.next_pos
        char = pi.next()
        if char.isalpha():
            pi.wind_back()
            text = _consume_identifier(pi)
            token_type = TokenType.identifier
        elif char.isspace():
            continue  # Skip over whitespace
        elif char == "%":
            next_char = pi.peek()
            if next_char is not None and next_char.isalpha():
                token_type = TokenType.placeholder
                text = "%" + _consume_identifier(pi)
            else:
                token_type = TokenType.punctuation
                text = "%"
        elif char in starting_char_to_continuations:
            token_type = TokenType.punctuation
            continuations = starting_char_to_continuations[char]
            if not pi.has_next():
                if "" in continuations:
                    text = char
                else:
                    raise TokenizeError(
                        f"unexpected EOF following {char} (expected one of"
                        f" {continuations})"
                    )
            else:
                c = pi.next()
                if c in continuations:
                    text = char + c
                    if text == "--":
                        token_type = TokenType.comment
                        text += _consume_until(pi, "\n", eof_okay=True)
                    elif text == "/*":
                        token_type = TokenType.comment
                        chars = []
                        seen_star = False
                        for c in pi:
                            chars.append(c)
                            if seen_star and c == "/":
                                text += "".join(chars)
                                break
                            if c == "*":
                                seen_star = True
                            else:
                                seen_star = False
                        else:
                            raise TokenizeError("unexpected EOF (expected '*/')")
                elif "" in c:
                    pi.wind_back()
                    text = char
                else:
                    raise TokenizeError(
                        f"unexpected {c} following {char} (expected one of"
                        f" {continuations})"
                    )
        elif char.isnumeric():
            # TODO floats, hex, other kinds of numbers?
            pi.wind_back()
            token_type = TokenType.number
            text = _consume_integer(pi)
        elif char in QUOTATIONS:
            token_type = TokenType.string
            text = char + _consume_until(pi, char)
        elif char == "{":
            token_type = TokenType.placeholder
            text = "{" + _consume_until(pi, "}")
        elif char == "#":
            token_type = TokenType.comment
            text = "#" + _consume_until(pi, "\n", eof_okay=True)
        else:
            # TODO comments. Maybe we leave those out of the AST in the parser, then reattach
            # them later at the nearest node we find. Or we just attach them to the token and make
            # sure each token object stays in the AST.
            raise TokenizeError(f"unexpected character {char}")
        yield Token(token_type, text, Location(sql, start_index, pi.next_pos - 1))


def _consume_until(pi: PeekingIterator[str], end: str, eof_okay: bool = False) -> str:
    chars = []
    for c in pi:
        chars.append(c)
        # TODO backslash escapes?
        if c == end:
            return "".join(chars)
    if eof_okay:
        return "".join(chars)
    raise TokenizeError(f"unexpected EOF (expected {end!r})")


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
