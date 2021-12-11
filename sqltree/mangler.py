"""

The mangler mangles the token list to make it easier for the
parser to handle.

Makes the following changes:

- Attaches all comments (except at the beginning of the string) to
  a neighboring token.
- Turns the following pairs of keywords into a single token:
  IS NOT, NOT IN, NOT LIKE, NOT REGEXP

"""
from typing import Iterable

from .location import Location
from .tokenizer import Token, TokenType

KEYWORD_PAIRS = [("IS", "NOT"), ("NOT", "IN"), ("NOT", "LIKE"), ("NOT", "REGEXP")]


def mangle(tokens: Iterable[Token]) -> Iterable[Token]:
    new_tokens = []
    for i, token in enumerate(tokens):
        if i == 0:
            new_tokens.append(token)
            continue
        mangled = False
        for left_kw, right_kw in KEYWORD_PAIRS:
            if _is_keyword(token, right_kw) and _is_keyword(new_tokens[-1], left_kw):
                new_tokens[-1] = _merge_tokens(new_tokens[-1], token)
                mangled = True
                break
        if mangled:
            continue
        if token.typ is TokenType.comment:
            old_token = new_tokens[-1]
            new_tokens[-1] = Token(
                old_token.typ,
                old_token.text,
                old_token.loc,
                [*old_token.comments, token],
            )
            continue
        new_tokens.append(token)
    return new_tokens


def _is_keyword(token: Token, keyword: str) -> bool:
    return token.typ is TokenType.keyword and token.text == keyword


def _merge_tokens(left: Token, right: Token) -> Token:
    loc = Location(left.loc.sql, left.loc.start_index, right.loc.end_index)
    return Token(
        TokenType.keyword,
        f"{left.text} {right.text}",
        loc,
        [*left.comments, *right.comments],
    )
