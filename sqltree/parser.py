from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence, Tuple, Union

from sqltree.peeking_iterator import PeekingIterator

from .location import Location
from .tokenizer import Token, TokenType


@dataclass
class EOFError(Exception):
    expected: str

    def __post_init__(self) -> None:
        super().__init__(str(self))

    def __str__(self) -> str:
        return f"Got EOF (expecting {self.expected!r})"


@dataclass
class ParseError(Exception):
    message: str
    location: Location

    def __post_init__(self) -> None:
        super().__init__(self.message)

    @classmethod
    def from_unexpected_token(cls, token: Token, expected: str) -> "ParseError":
        return ParseError(f"Unexpected {token.text!r} (expected {expected})", token.loc)


@dataclass
class Node:
    pass


@dataclass
class Leaf(Node):
    token: Token = field(repr=False, compare=False)


@dataclass
class Keyword(Leaf):
    text: str


@dataclass
class Punctuation(Leaf):
    text: str


@dataclass
class Expression(Node):
    pass


@dataclass
class Identifier(Expression, Leaf):
    text: str


@dataclass
class StringLiteral(Expression, Leaf):
    value: str


@dataclass
class IntegerLiteral(Expression, Leaf):
    value: int


@dataclass
class Placeholder(Expression, Leaf):
    text: str


@dataclass
class BinOp(Expression):
    left: Expression
    op: Union[Punctuation, Keyword]
    right: Expression


@dataclass
class Parenthesized(Expression):
    left_punc: Punctuation = field(compare=False, repr=False)
    inner: Expression
    right_punc: Punctuation = field(compare=False, repr=False)


@dataclass
class Star(Expression, Leaf):
    pass


@dataclass
class SelectExpr(Node):
    expr: Expression
    as_kw: Optional[Keyword] = field(compare=False, repr=False)
    alias: Optional[Identifier]
    trailing_comma: Optional[Punctuation] = field(compare=False, repr=False)


@dataclass
class Statement(Node):
    pass


@dataclass
class Select(Statement):
    select_kw: Keyword = field(compare=False, repr=False)
    select_exprs: Sequence[SelectExpr]
    from_kw: Optional[Keyword] = field(compare=False, repr=False)
    table: Optional[Expression]
    where_kw: Optional[Keyword] = field(compare=False, repr=False)
    conditions: Optional[Expression]


def parse(tokens: Iterable[Token]) -> Statement:
    pi = PeekingIterator(list(tokens))
    first = pi.peek()
    if first is None:
        raise ValueError("SQL is empty")
    elif first.typ is TokenType.keyword:
        if first.text == "SELECT":
            statement = _parse_select(pi)
        else:
            raise ParseError(f"Unexpected {first.text!r}", first.loc)
        remaining = pi.peek()
        if remaining is not None:
            raise ParseError.from_unexpected_token(remaining, "EOF")
        return statement
    else:
        raise ParseError(f"Unexpected {first.text!r}", first.loc)


def _parse_select(pi: PeekingIterator[Token]) -> Select:
    select = _expect_keyword(pi, "SELECT")
    select_exprs = []
    from_kw = table = where_kw = conditions = None
    while True:
        expr = _parse_select_expr(pi)
        select_exprs.append(expr)
        if expr.trailing_comma is None:
            break

    if _next_is_keyword(pi, "FROM"):
        from_kw = _expect_keyword(pi, "FROM")
        table = _parse_expression(pi)

    if _next_is_keyword(pi, "WHERE"):
        where_kw = _expect_keyword(pi, "WHERE")
        conditions = _parse_expression(pi)

    return Select(select, select_exprs, from_kw, table, where_kw, conditions)


def _parse_select_expr(pi: PeekingIterator[Token]) -> SelectExpr:
    expr = _parse_expression(pi)
    as_kw = alias = trailing_comma = None
    if _next_is_keyword(pi, "AS"):
        as_kw = _expect_keyword(pi, "AS")
        token = _next_or_else(pi, "identifier")
        if token.typ is not TokenType.identifier:
            raise ParseError.from_unexpected_token(token, "identifier")
        alias = Identifier(token, token.text)
    if _next_is_punctuation(pi, ","):
        trailing_comma = _expect_punctuation(pi, ",")
    return SelectExpr(expr, as_kw, alias, trailing_comma)


def K(text: str) -> Tuple[TokenType, str]:
    return (TokenType.keyword, text)


def P(text: str) -> Tuple[TokenType, str]:
    return (TokenType.punctuation, text)


_BINOP_PRECEDENCE = [
    (P("^"),),
    (P("*"), P("/"), P("DIV"), P("%"), P("MOD")),
    (P("-"), P("+")),
    (P("<<"), P(">>")),
    (P("&"),),
    (P("|"),),
    (
        P("="),
        P("<=>"),
        P(">="),
        P(">"),
        P("<="),
        P("<"),
        P("<>"),
        P("!="),
        K("IS"),
        K("LIKE"),
        K("REGEXP"),
        K("IN"),
    ),
    # TODO: BETWEEN, CASE, WHEN, THEN, ELSE
    (K("AND"), P("&&")),
    (K("XOR"),),
    (K("OR"), P("||")),
]
_MIN_PRECEDENCE = len(_BINOP_PRECEDENCE) - 1


def _parse_expression(pi: PeekingIterator[Token]) -> Expression:
    return _parse_binop(pi, _MIN_PRECEDENCE)


def _parse_binop(pi: PeekingIterator[Token], precedence: int) -> Expression:
    if precedence == 0:
        left = _parse_simple_expression(pi)
    else:
        left = _parse_binop(pi, precedence - 1)
    while True:
        token = pi.peek()
        if token is None:
            return left
        options = _BINOP_PRECEDENCE[precedence]
        if any(token.typ is typ and token.text == text for typ, text in options):
            pi.next()
            if token.typ is TokenType.punctuation:
                op = Punctuation(token, token.text)
            else:
                assert token.typ is TokenType.keyword
                op = Keyword(token, token.text)
            right = _parse_binop(pi, precedence - 1)
            left = BinOp(left, op, right)
        else:
            return left


def _parse_simple_expression(pi: PeekingIterator[Token]) -> Expression:
    token = _next_or_else(pi, "expression")
    if token.typ is TokenType.punctuation and token.text == "*":
        return Star(token)
    elif token.typ is TokenType.punctuation and token.text == "(":
        inner = _parse_expression(pi)
        right = _expect_punctuation(pi, ")")
        return Parenthesized(Punctuation(token, "("), inner, right)
    elif token.typ is TokenType.identifier:
        return Identifier(token, token.text)
    elif token.typ is TokenType.number:
        return IntegerLiteral(token, int(token.text))
    elif token.typ is TokenType.placeholder:
        return Placeholder(token, token.text)
    elif token.typ is TokenType.string:
        text = token.text[1:-1]
        if token.text[0] == "`":
            return Identifier(token, text)
        else:
            return StringLiteral(token, text)
    else:
        raise ParseError.from_unexpected_token(token, "expression")


def _next_is_punctuation(pi: PeekingIterator[Token], punctuation: str) -> bool:
    token = pi.peek()
    return (
        token is not None
        and token.typ is TokenType.punctuation
        and token.text == punctuation
    )


def _expect_punctuation(pi: PeekingIterator[Token], punctuation: str) -> Punctuation:
    token = _next_or_else(pi, punctuation)
    if token.typ is not TokenType.punctuation or token.text != punctuation:
        raise ParseError.from_unexpected_token(token, repr(punctuation))
    return Punctuation(token, token.text)


def _next_is_keyword(pi: PeekingIterator[Token], keyword: str) -> bool:
    token = pi.peek()
    return (
        token is not None and token.typ is TokenType.keyword and token.text == keyword
    )


def _expect_keyword(pi: PeekingIterator[Token], keyword: str) -> Keyword:
    token = _next_or_else(pi, keyword)
    if token.typ is not TokenType.keyword or token.text != keyword:
        raise ParseError.from_unexpected_token(token, repr(keyword))
    return Keyword(token, token.text)


def _next_or_else(pi: PeekingIterator[Token], label: str) -> Token:
    try:
        return pi.next()
    except StopIteration:
        raise EOFError(label)
