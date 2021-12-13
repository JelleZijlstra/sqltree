from dataclasses import dataclass, field, replace
from typing import Iterable, Optional, Sequence, Tuple, Union

from .keywords import MYSQL_KEYWORDS
from .location import Location
from .peeking_iterator import PeekingIterator
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
        self.location.display()

    @classmethod
    def from_unexpected_token(cls, token: Token, expected: str) -> "ParseError":
        return ParseError(f"Unexpected {token.text!r} (expected {expected})", token.loc)

    def __str__(self) -> str:
        return f"{self.message}\n{self.location.display().rstrip()}"


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
class KeywordSequence(Node):
    keywords: Sequence[Keyword]


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
class OrderByExpr(Node):
    """Also used for GROUP BY."""

    expr: Expression
    direction_kw: Optional[Keyword] = field(compare=False, repr=False)
    trailing_comma: Optional[Punctuation] = field(compare=False, repr=False)


@dataclass
class Comment(Leaf):
    text: str


@dataclass
class FromClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    table: Expression


@dataclass
class WhereClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    conditions: Expression


@dataclass
class GroupByClause(Node):
    kwseq: KeywordSequence = field(compare=False, repr=False)
    expr: Sequence[OrderByExpr]


@dataclass
class HavingClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    conditions: Expression


@dataclass
class OrderByClause(Node):
    kwseq: KeywordSequence = field(compare=False, repr=False)
    expr: Sequence[OrderByExpr]


@dataclass
class LimitClause(Node):
    # TODO placeholders, offsets
    kw: Keyword = field(compare=False, repr=False)
    row_count: IntegerLiteral


@dataclass
class Statement(Node):
    leading_comments: Sequence[Comment] = field(repr=False)


@dataclass
class Select(Statement):
    select_kw: Keyword = field(compare=False, repr=False)
    select_exprs: Sequence[SelectExpr]
    from_clause: Optional[FromClause] = None
    where: Optional[WhereClause] = None
    group_by: Optional[GroupByClause] = None
    having: Optional[HavingClause] = None
    order_by: Optional[OrderByClause] = None
    # TODO LIMIT


@dataclass
class Delete(Statement):
    delete_kw: Keyword = field(compare=False, repr=False)
    from_clause: FromClause
    where: Optional[WhereClause] = None
    order_by: Optional[OrderByClause] = None
    limit: Optional[LimitClause] = None


@dataclass
class Default(Node):
    kw: Keyword = field(compare=False, repr=False)


Value = Union[Expression, Default]


@dataclass
class Assignment(Node):
    col_name: Identifier
    eq_punc: Punctuation
    value: Value
    trailing_comma: Optional[Punctuation] = None


@dataclass
class SetClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    assignments: Sequence[Assignment]


@dataclass
class Update(Statement):
    update_kw: Keyword = field(compare=False, repr=False)
    table: Expression
    set_clause: SetClause
    where: Optional[WhereClause] = None
    order_by: Optional[OrderByClause] = None
    limit: Optional[LimitClause] = None


@dataclass
class ColName(Node):
    col_name: Identifier
    trailing_comma: Optional[Punctuation] = None


@dataclass
class IntoClause(Node):
    kw: Optional[Keyword] = field(compare=False, repr=False)
    table: Expression
    open_paren: Optional[Punctuation]
    col_names: Sequence[ColName]
    close_paren: Optional[Punctuation]


@dataclass
class ValueWithComma(Node):
    value: Value
    trailing_comma: Optional[Punctuation] = None


@dataclass
class ValueList(Node):
    open_paren: Punctuation
    values: Sequence[ValueWithComma]
    close_paren: Punctuation
    trailing_comma: Optional[Punctuation] = None


@dataclass
class ValuesClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    value_lists: Sequence[ValueList]


@dataclass
class OdkuClause(Node):
    kwseq: KeywordSequence
    assignments: Sequence[Assignment]


@dataclass
class Insert(Statement):
    insert_kw: Keyword = field(compare=False, repr=False)
    ignore_kw: Optional[Keyword]
    into: IntoClause
    values: ValuesClause
    odku: Optional[OdkuClause] = None


@dataclass
class Replace(Statement):
    replace_kw: Keyword = field(compare=False, repr=False)
    into: IntoClause
    values: ValuesClause


def parse(tokens: Iterable[Token]) -> Statement:
    pi = PeekingIterator(list(tokens))
    return _parse_statement(pi)


def _parse_statement(pi: PeekingIterator[Token]) -> Statement:
    first = pi.peek()
    if first is None:
        raise ValueError("SQL is empty")
    elif first.typ is TokenType.keyword:
        try:
            parser = _VERB_TO_PARSER[first.text]
        except KeyError:
            raise ParseError(f"Unexpected {first.text!r}", first.loc)
        else:
            statement = parser(pi)
        remaining = pi.peek()
        if remaining is not None:
            raise ParseError.from_unexpected_token(remaining, "EOF")
        return statement
    elif first.typ is TokenType.comment:
        pi.next()
        comment = Comment(first, first.text)
        statement = _parse_statement(pi)
        leading_comments = (comment, *statement.leading_comments)
        return replace(statement, leading_comments=leading_comments)
    else:
        raise ParseError(f"Unexpected {first.text!r}", first.loc)


def _parse_from_clause(pi: PeekingIterator[Token]) -> Optional[FromClause]:
    if _next_is_keyword(pi, "FROM"):
        from_kw = _expect_keyword(pi, "FROM")
        table = _parse_expression(pi)
        return FromClause(from_kw, table)
    return None


def _parse_where_clause(pi: PeekingIterator[Token]) -> Optional[WhereClause]:
    if _next_is_keyword(pi, "WHERE"):
        kw = _expect_keyword(pi, "WHERE")
        table = _parse_expression(pi)
        return WhereClause(kw, table)
    return None


def _parse_having_clause(pi: PeekingIterator[Token]) -> Optional[HavingClause]:
    if _next_is_keyword(pi, "HAVING"):
        kw = _expect_keyword(pi, "HAVING")
        table = _parse_expression(pi)
        return HavingClause(kw, table)
    return None


def _parse_group_by_clause(pi: PeekingIterator[Token]) -> Optional[GroupByClause]:
    kwseq = _maybe_consume_keyword_sequence(pi, ["GROUP", "BY"])
    if kwseq is not None:
        exprs = _parse_order_by_list(pi)
        return GroupByClause(kwseq, exprs)
    else:
        return None


def _parse_order_by_clause(pi: PeekingIterator[Token]) -> Optional[OrderByClause]:
    kwseq = _maybe_consume_keyword_sequence(pi, ["ORDER", "BY"])
    if kwseq is not None:
        exprs = _parse_order_by_list(pi)
        return OrderByClause(kwseq, exprs)
    else:
        return None


def _parse_assignment_list(pi: PeekingIterator[Token]) -> Sequence[Assignment]:
    assignments = []
    while True:
        expr = _parse_assignment(pi)
        assignments.append(expr)
        if expr.trailing_comma is None:
            break
    return assignments


def _parse_set_clause(pi: PeekingIterator[Token]) -> SetClause:
    kw = _expect_keyword(pi, "SET")
    assignments = _parse_assignment_list(pi)
    return SetClause(kw, assignments)


def _parse_limit_clause(pi: PeekingIterator[Token]) -> Optional[LimitClause]:
    if _next_is_keyword(pi, "LIMIT"):
        kw = _expect_keyword(pi, "LIMIT")
        token = _next_or_else(pi, "number")
        if token.typ is not TokenType.number:
            raise ParseError.from_unexpected_token(token, "number")
        return LimitClause(kw, IntegerLiteral(token, int(token.text)))
    return None


def _parse_update(pi: PeekingIterator[Token]) -> Update:
    kw = _expect_keyword(pi, "UPDATE")
    table = _parse_expression(pi)
    set_clause = _parse_set_clause(pi)
    where_clause = _parse_where_clause(pi)
    order_by_clause = _parse_order_by_clause(pi)
    limit_clause = _parse_limit_clause(pi)
    return Update(
        (), kw, table, set_clause, where_clause, order_by_clause, limit_clause
    )


def _parse_delete(pi: PeekingIterator[Token]) -> Delete:
    delete = _expect_keyword(pi, "DELETE")
    from_clause = _parse_from_clause(pi)
    if from_clause is None:
        token = _next_or_else(pi, "FROM")
        raise ParseError.from_unexpected_token(token, "FROM")
    where_clause = _parse_where_clause(pi)
    order_by_clause = _parse_order_by_clause(pi)
    limit_clause = _parse_limit_clause(pi)
    return Delete((), delete, from_clause, where_clause, order_by_clause, limit_clause)


def _parse_select(pi: PeekingIterator[Token]) -> Select:
    select = _expect_keyword(pi, "SELECT")
    select_exprs = []
    while True:
        expr = _parse_select_expr(pi)
        select_exprs.append(expr)
        if expr.trailing_comma is None:
            break

    from_clause = _parse_from_clause(pi)
    where_clause = _parse_where_clause(pi)
    group_by_clause = _parse_group_by_clause(pi)
    having_clause = _parse_having_clause(pi)
    order_by_clause = _parse_order_by_clause(pi)

    return Select(
        (),
        select,
        select_exprs,
        from_clause,
        where_clause,
        group_by_clause,
        having_clause,
        order_by_clause,
    )


def _parse_col_name(pi: PeekingIterator[Token]) -> ColName:
    name = _parse_identifier(pi)
    comma = _maybe_consume_punctuation(pi, ",")
    return ColName(name, comma)


def _parse_into_clause(pi: PeekingIterator[Token]) -> IntoClause:
    into = _maybe_consume_keyword(pi, "INTO")  # INTO is optional, at least in MYSQL
    table = _parse_expression(pi)
    if _next_is_punctuation(pi, "("):
        open_paren = _expect_punctuation(pi, "(")
        col_names = []
        while True:
            col_name = _parse_col_name(pi)
            col_names.append(col_name)
            if col_name.trailing_comma is None:
                break
        close_paren = _expect_punctuation(pi, ")")
    else:
        col_names = ()
        open_paren = close_paren = None
    return IntoClause(into, table, open_paren, col_names, close_paren)


def _parse_value_with_comma(pi: PeekingIterator[Token]) -> ValueWithComma:
    val = _parse_value(pi)
    comma = _maybe_consume_punctuation(pi, ",")
    return ValueWithComma(val, comma)


def _parse_value_list(pi: PeekingIterator[Token]) -> ValueList:
    open_paren = _expect_punctuation(pi, "(")
    values = []
    while True:
        value = _parse_value_with_comma(pi)
        values.append(value)
        if value.trailing_comma is None:
            break
    close_paren = _expect_punctuation(pi, ")")
    comma = _maybe_consume_punctuation(pi, ",")
    return ValueList(open_paren, values, close_paren, comma)


def _parse_values_clause(pi: PeekingIterator[Token]) -> ValuesClause:
    kw = _maybe_consume_soft_keyword(pi, "VALUE")
    if kw is None:
        kw = _expect_keyword(pi, "VALUES")
    value_lists = []
    while True:
        value_list = _parse_value_list(pi)
        value_lists.append(value_list)
        if value_list.trailing_comma is None:
            break
    return ValuesClause(kw, value_lists)


def _parse_odku_clause(pi: PeekingIterator[Token]) -> Optional[OdkuClause]:
    odku = _maybe_consume_keyword_sequence(pi, ["ON", "DUPLICATE", "KEY", "UPDATE"])
    if odku is None:
        return None
    assignments = _parse_assignment_list(pi)
    return OdkuClause(odku, assignments)


def _parse_insert(pi: PeekingIterator[Token]) -> Insert:
    insert = _expect_keyword(pi, "INSERT")
    ignore = _maybe_consume_keyword(pi, "IGNORE")
    into = _parse_into_clause(pi)
    values = _parse_values_clause(pi)
    odku = _parse_odku_clause(pi)
    return Insert((), insert, ignore, into, values, odku)


def _parse_replace(pi: PeekingIterator[Token]) -> Replace:
    insert = _expect_keyword(pi, "REPLACE")
    into = _parse_into_clause(pi)
    values = _parse_values_clause(pi)
    return Replace((), insert, into, values)


_VERB_TO_PARSER = {
    "SELECT": _parse_select,
    "UPDATE": _parse_update,
    "DELETE": _parse_delete,
    "INSERT": _parse_insert,
    "REPLACE": _parse_replace,
}


def _parse_identifier(pi: PeekingIterator[Token]) -> Identifier:
    token = _next_or_else(pi, "identifier")
    if token.typ is not TokenType.identifier:
        raise ParseError.from_unexpected_token(token, "identifier")
    return Identifier(token, token.text)


def _parse_select_expr(pi: PeekingIterator[Token]) -> SelectExpr:
    expr = _parse_expression(pi)
    as_kw = alias = trailing_comma = None
    if _next_is_keyword(pi, "AS"):
        as_kw = _expect_keyword(pi, "AS")
        alias = _parse_identifier(pi)
    if _next_is_punctuation(pi, ","):
        trailing_comma = _expect_punctuation(pi, ",")
    return SelectExpr(expr, as_kw, alias, trailing_comma)


def _parse_value(pi: PeekingIterator[Token]) -> Value:
    if _next_is_keyword(pi, "DEFAULT"):
        kw = _expect_keyword(pi, "DEFAULT")
        return Default(kw)
    else:
        return _parse_expression(pi)


def _parse_assignment(pi: PeekingIterator[Token]) -> Assignment:
    colname = _parse_identifier(pi)
    punc = _expect_punctuation(pi, "=")
    value = _parse_value(pi)
    if _next_is_punctuation(pi, ","):
        trailing_comma = _expect_punctuation(pi, ",")
    else:
        trailing_comma = None
    return Assignment(colname, punc, value, trailing_comma)


def _parse_order_by_list(pi: PeekingIterator[Token]) -> Sequence[OrderByExpr]:
    exprs = []
    while True:
        expr = _parse_order_by_expr(pi)
        exprs.append(expr)
        if expr.trailing_comma is None:
            return exprs


def _parse_order_by_expr(pi: PeekingIterator[Token]) -> OrderByExpr:
    expr = _parse_expression(pi)
    if _next_is_keyword(pi, "ASC"):
        direction = _expect_keyword(pi, "ASC")
    elif _next_is_keyword(pi, "DESC"):
        direction = _expect_keyword(pi, "DESC")
    else:
        direction = None
    trailing_comma = _maybe_consume_punctuation(pi, ",")
    return OrderByExpr(expr, direction, trailing_comma)


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
        K("IS NOT"),
        K("NOT LIKE"),
        K("LIKE"),
        K("NOT REGEXP"),
        K("REGEXP"),
        K("NOT IN"),
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


def _maybe_consume_punctuation(
    pi: PeekingIterator[Token], punctuation: str
) -> Optional[Punctuation]:
    for token in pi:
        if token.typ is TokenType.punctuation and token.text == punctuation:
            return Punctuation(token, token.text)
        else:
            pi.wind_back()
            break
    return None


def _expect_punctuation(pi: PeekingIterator[Token], punctuation: str) -> Punctuation:
    token = _next_or_else(pi, punctuation)
    if token.typ is not TokenType.punctuation or token.text != punctuation:
        raise ParseError.from_unexpected_token(token, repr(punctuation))
    return Punctuation(token, token.text)


def _maybe_consume_keyword_sequence(
    pi: PeekingIterator[Token], keywords: Sequence[str]
) -> Optional[KeywordSequence]:
    keywords_found = []
    consumed = 0
    for token in pi:
        consumed += 1
        expected = keywords[len(keywords_found)]
        if expected in MYSQL_KEYWORDS:
            condition = token.typ is TokenType.keyword and token.text == expected
        else:
            condition = (
                token.typ is TokenType.identifier and token.text.upper() == expected
            )
        if condition:
            keywords_found.append(token)
            if len(keywords_found) == len(keywords):
                return KeywordSequence(
                    [Keyword(token, token.text) for token in keywords_found]
                )
        else:
            break

    for _ in range(consumed):
        pi.wind_back()
    return None


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


def _maybe_consume_keyword(
    pi: PeekingIterator[Token], keyword: str
) -> Optional[Keyword]:
    for token in pi:
        if token.typ is TokenType.keyword and token.text == keyword:
            return Keyword(token, token.text)
        else:
            pi.wind_back()
            break
    return None


def _maybe_consume_soft_keyword(
    pi: PeekingIterator[Token], keyword: str
) -> Optional[Keyword]:
    for token in pi:
        if token.typ is TokenType.identifier and token.text.upper() == keyword:
            return Keyword(token, token.text)
        else:
            pi.wind_back()
            break
    return None


def _next_or_else(pi: PeekingIterator[Token], label: str) -> Token:
    try:
        return pi.next()
    except StopIteration:
        raise EOFError(label)
