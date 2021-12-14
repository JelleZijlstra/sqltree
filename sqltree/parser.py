from dataclasses import dataclass, field, replace
from typing import Iterable, Optional, Sequence, Tuple, Union

from sqltree.dialect import Dialect

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
class Parser:
    pi: PeekingIterator[Token]
    dialect: Dialect


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
    kw: Keyword = field(compare=False, repr=False)
    row_count: Expression


@dataclass
class SelectLimitClause(Node):
    """A LIMIT clause on a SELECT statement, which supports more features.

    There are two syntaxes for LIMIT with an offset:

        LIMIT offset, row_count
        LIMIT row_count OFFSET offset

    We represent these by putting either the OFFSET keyword or trailing comma
    punctuation in the offset_leaf field.

    """

    kw: Keyword = field(compare=False, repr=False)
    row_count: Expression
    offset: Optional[Expression] = None
    offset_leaf: Union[Punctuation, Keyword, None] = None


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
    limit: Optional[SelectLimitClause] = None


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


def parse(tokens: Iterable[Token], dialect: Dialect) -> Statement:
    p = Parser(PeekingIterator(list(tokens)), dialect)
    return _parse_statement(p)


def _parse_statement(p: Parser) -> Statement:
    first = p.pi.peek()
    if first is None:
        raise ValueError("SQL is empty")
    elif first.typ is TokenType.keyword:
        try:
            parser = _VERB_TO_PARSER[first.text]
        except KeyError:
            raise ParseError(f"Unexpected {first.text!r}", first.loc)
        else:
            statement = parser(p)
        remaining = p.pi.peek()
        if remaining is not None:
            raise ParseError.from_unexpected_token(remaining, "EOF")
        return statement
    # In Redshift, INSERT is a soft keyword, so INSERT wouldn't match the previous expression.
    elif first.typ is TokenType.identifier:
        try:
            parser = _VERB_TO_PARSER[first.text.upper()]
        except KeyError:
            raise ParseError(f"Unexpected {first.text!r}", first.loc)
        else:
            statement = parser(p)
        remaining = p.pi.peek()
        if remaining is not None:
            raise ParseError.from_unexpected_token(remaining, "EOF")
        return statement
    elif first.typ is TokenType.comment:
        p.pi.next()
        comment = Comment(first, first.text)
        statement = _parse_statement(p)
        leading_comments = (comment, *statement.leading_comments)
        return replace(statement, leading_comments=leading_comments)
    else:
        raise ParseError(f"Unexpected {first.text!r}", first.loc)


def _parse_from_clause(p: Parser) -> Optional[FromClause]:
    from_kw = _maybe_consume_keyword(p, "FROM")
    if from_kw is not None:
        table = _parse_expression(p)
        return FromClause(from_kw, table)
    return None


def _parse_where_clause(p: Parser) -> Optional[WhereClause]:
    kw = _maybe_consume_keyword(p, "WHERE")
    if kw is not None:
        table = _parse_expression(p)
        return WhereClause(kw, table)
    return None


def _parse_having_clause(p: Parser) -> Optional[HavingClause]:
    kw = _maybe_consume_keyword(p, "HAVING")
    if kw is not None:
        table = _parse_expression(p)
        return HavingClause(kw, table)
    return None


def _parse_group_by_clause(p: Parser) -> Optional[GroupByClause]:
    kwseq = _maybe_consume_keyword_sequence(p, ["GROUP", "BY"])
    if kwseq is not None:
        exprs = _parse_order_by_list(p)
        return GroupByClause(kwseq, exprs)
    else:
        return None


def _parse_order_by_clause(p: Parser) -> Optional[OrderByClause]:
    kwseq = _maybe_consume_keyword_sequence(p, ["ORDER", "BY"])
    if kwseq is not None:
        exprs = _parse_order_by_list(p)
        return OrderByClause(kwseq, exprs)
    else:
        return None


def _parse_assignment_list(p: Parser) -> Sequence[Assignment]:
    assignments = []
    while True:
        expr = _parse_assignment(p)
        assignments.append(expr)
        if expr.trailing_comma is None:
            break
    return assignments


def _parse_set_clause(p: Parser) -> SetClause:
    kw = _expect_keyword(p, "SET")
    assignments = _parse_assignment_list(p)
    return SetClause(kw, assignments)


def _parse_limit_clause(p: Parser) -> Optional[LimitClause]:
    kw = _maybe_consume_keyword(p, "LIMIT")
    if kw is not None:
        expr = _parse_simple_expression(p)
        return LimitClause(kw, expr)
    return None


def _parse_select_limit_clause(p: Parser) -> Optional[SelectLimitClause]:
    kw = _maybe_consume_keyword(p, "LIMIT")
    if kw is not None:
        expr = _parse_simple_expression(p)
        if _next_is_punctuation(p, ","):
            offset_leaf = _expect_punctuation(p, ",")
            offset = expr
            expr = _parse_simple_expression(p)
        else:
            offset_leaf = _maybe_consume_keyword(p, "OFFSET")
            if offset_leaf is not None:
                offset = _parse_simple_expression(p)
            else:
                offset = None
        return SelectLimitClause(kw, expr, offset, offset_leaf)
    return None


def _parse_update(p: Parser) -> Update:
    kw = _expect_keyword(p, "UPDATE")
    table = _parse_expression(p)
    set_clause = _parse_set_clause(p)
    where_clause = _parse_where_clause(p)
    order_by_clause = _parse_order_by_clause(p)
    limit_clause = _parse_limit_clause(p)
    return Update(
        (), kw, table, set_clause, where_clause, order_by_clause, limit_clause
    )


def _parse_delete(p: Parser) -> Delete:
    delete = _expect_keyword(p, "DELETE")
    from_clause = _parse_from_clause(p)
    if from_clause is None:
        token = _next_or_else(p, "FROM")
        raise ParseError.from_unexpected_token(token, "FROM")
    where_clause = _parse_where_clause(p)
    order_by_clause = _parse_order_by_clause(p)
    limit_clause = _parse_limit_clause(p)
    return Delete((), delete, from_clause, where_clause, order_by_clause, limit_clause)


def _parse_select(p: Parser) -> Select:
    select = _expect_keyword(p, "SELECT")
    select_exprs = []
    while True:
        expr = _parse_select_expr(p)
        select_exprs.append(expr)
        if expr.trailing_comma is None:
            break

    from_clause = _parse_from_clause(p)
    where_clause = _parse_where_clause(p)
    group_by_clause = _parse_group_by_clause(p)
    having_clause = _parse_having_clause(p)
    order_by_clause = _parse_order_by_clause(p)
    select_limit_clause = _parse_select_limit_clause(p)

    return Select(
        (),
        select,
        select_exprs,
        from_clause,
        where_clause,
        group_by_clause,
        having_clause,
        order_by_clause,
        select_limit_clause,
    )


def _parse_col_name(p: Parser) -> ColName:
    name = _parse_identifier(p)
    comma = _maybe_consume_punctuation(p, ",")
    return ColName(name, comma)


def _parse_into_clause(p: Parser) -> IntoClause:
    into = _maybe_consume_keyword(p, "INTO")  # INTO is optional, at least in MYSQL
    table = _parse_expression(p)
    if _next_is_punctuation(p, "("):
        open_paren = _expect_punctuation(p, "(")
        col_names = []
        while True:
            col_name = _parse_col_name(p)
            col_names.append(col_name)
            if col_name.trailing_comma is None:
                break
        close_paren = _expect_punctuation(p, ")")
    else:
        col_names = ()
        open_paren = close_paren = None
    return IntoClause(into, table, open_paren, col_names, close_paren)


def _parse_value_with_comma(p: Parser) -> ValueWithComma:
    val = _parse_value(p)
    comma = _maybe_consume_punctuation(p, ",")
    return ValueWithComma(val, comma)


def _parse_value_list(p: Parser) -> ValueList:
    open_paren = _expect_punctuation(p, "(")
    values = []
    while True:
        value = _parse_value_with_comma(p)
        values.append(value)
        if value.trailing_comma is None:
            break
    close_paren = _expect_punctuation(p, ")")
    comma = _maybe_consume_punctuation(p, ",")
    return ValueList(open_paren, values, close_paren, comma)


def _parse_values_clause(p: Parser) -> ValuesClause:
    kw = _maybe_consume_keyword(p, "VALUE")
    if kw is None:
        kw = _expect_keyword(p, "VALUES")
    value_lists = []
    while True:
        value_list = _parse_value_list(p)
        value_lists.append(value_list)
        if value_list.trailing_comma is None:
            break
    return ValuesClause(kw, value_lists)


def _parse_odku_clause(p: Parser) -> Optional[OdkuClause]:
    odku = _maybe_consume_keyword_sequence(p, ["ON", "DUPLICATE", "KEY", "UPDATE"])
    if odku is None:
        return None
    assignments = _parse_assignment_list(p)
    return OdkuClause(odku, assignments)


def _parse_insert(p: Parser) -> Insert:
    insert = _expect_keyword(p, "INSERT")
    ignore = _maybe_consume_keyword(p, "IGNORE")
    into = _parse_into_clause(p)
    values = _parse_values_clause(p)
    odku = _parse_odku_clause(p)
    return Insert((), insert, ignore, into, values, odku)


def _parse_replace(p: Parser) -> Replace:
    insert = _expect_keyword(p, "REPLACE")
    into = _parse_into_clause(p)
    values = _parse_values_clause(p)
    return Replace((), insert, into, values)


_VERB_TO_PARSER = {
    "SELECT": _parse_select,
    "UPDATE": _parse_update,
    "DELETE": _parse_delete,
    "INSERT": _parse_insert,
    "REPLACE": _parse_replace,
}


def _parse_identifier(p: Parser) -> Identifier:
    token = _next_or_else(p, "identifier")
    if token.typ is not TokenType.identifier:
        raise ParseError.from_unexpected_token(token, "identifier")
    return Identifier(token, token.text)


def _parse_select_expr(p: Parser) -> SelectExpr:
    expr = _parse_expression(p)
    alias = trailing_comma = None
    as_kw = _maybe_consume_keyword(p, "AS")
    if as_kw is not None:
        alias = _parse_identifier(p)
    if _next_is_punctuation(p, ","):
        trailing_comma = _expect_punctuation(p, ",")
    return SelectExpr(expr, as_kw, alias, trailing_comma)


def _parse_value(p: Parser) -> Value:
    kw = _maybe_consume_keyword(p, "DEFAULT")
    if kw is not None:
        return Default(kw)
    else:
        return _parse_expression(p)


def _parse_assignment(p: Parser) -> Assignment:
    colname = _parse_identifier(p)
    punc = _expect_punctuation(p, "=")
    value = _parse_value(p)
    if _next_is_punctuation(p, ","):
        trailing_comma = _expect_punctuation(p, ",")
    else:
        trailing_comma = None
    return Assignment(colname, punc, value, trailing_comma)


def _parse_order_by_list(p: Parser) -> Sequence[OrderByExpr]:
    exprs = []
    while True:
        expr = _parse_order_by_expr(p)
        exprs.append(expr)
        if expr.trailing_comma is None:
            return exprs


def _parse_order_by_expr(p: Parser) -> OrderByExpr:
    expr = _parse_expression(p)
    direction = _maybe_consume_keyword(p, "ASC")
    if direction is None:
        direction = _maybe_consume_keyword(p, "DESC")
    trailing_comma = _maybe_consume_punctuation(p, ",")
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


def _parse_expression(p: Parser) -> Expression:
    return _parse_binop(p, _MIN_PRECEDENCE)


def _parse_binop(p: Parser, precedence: int) -> Expression:
    if precedence == 0:
        left = _parse_simple_expression(p)
    else:
        left = _parse_binop(p, precedence - 1)
    while True:
        token = p.pi.peek()
        if token is None:
            return left
        options = _BINOP_PRECEDENCE[precedence]
        if any(token.typ is typ and token.text == text for typ, text in options):
            p.pi.next()
            if token.typ is TokenType.punctuation:
                op = Punctuation(token, token.text)
            else:
                assert token.typ is TokenType.keyword
                op = Keyword(token, token.text)
            right = _parse_binop(p, precedence - 1)
            left = BinOp(left, op, right)
        else:
            return left


def _parse_simple_expression(p: Parser) -> Expression:
    token = _next_or_else(p, "expression")
    if token.typ is TokenType.punctuation and token.text == "*":
        return Star(token)
    elif token.typ is TokenType.punctuation and token.text == "(":
        inner = _parse_expression(p)
        right = _expect_punctuation(p, ")")
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


def _next_is_punctuation(p: Parser, punctuation: str) -> bool:
    token = p.pi.peek()
    return (
        token is not None
        and token.typ is TokenType.punctuation
        and token.text == punctuation
    )


def _maybe_consume_punctuation(p: Parser, punctuation: str) -> Optional[Punctuation]:
    for token in p.pi:
        if token.typ is TokenType.punctuation and token.text == punctuation:
            return Punctuation(token, token.text)
        else:
            p.pi.wind_back()
            break
    return None


def _expect_punctuation(p: Parser, punctuation: str) -> Punctuation:
    token = _next_or_else(p, punctuation)
    if token.typ is not TokenType.punctuation or token.text != punctuation:
        raise ParseError.from_unexpected_token(token, repr(punctuation))
    return Punctuation(token, token.text)


def _maybe_consume_keyword_sequence(
    p: Parser, keywords: Sequence[str]
) -> Optional[KeywordSequence]:
    keywords_found = []
    consumed = 0
    for token in p.pi:
        consumed += 1
        expected = keywords[len(keywords_found)]
        if expected in p.dialect.get_keywords():
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
        p.pi.wind_back()
    return None


def _expect_keyword(p: Parser, keyword: str) -> Keyword:
    is_hard = keyword in p.dialect.get_keywords()
    token = _next_or_else(p, keyword)
    if is_hard:
        condition = token.typ is TokenType.keyword and token.text == keyword
    else:
        condition = token.typ is TokenType.identifier and token.text.upper() == keyword
    if not condition:
        raise ParseError.from_unexpected_token(token, repr(keyword))
    return Keyword(token, token.text)


def _maybe_consume_keyword(p: Parser, keyword: str) -> Optional[Keyword]:
    is_hard = keyword in p.dialect.get_keywords()
    for token in p.pi:
        if is_hard:
            condition = token.typ is TokenType.keyword and token.text == keyword
        else:
            condition = (
                token.typ is TokenType.identifier and token.text.upper() == keyword
            )

        if condition:
            return Keyword(token, token.text)
        else:
            p.pi.wind_back()
            break
    return None


def _next_or_else(p: Parser, label: str) -> Token:
    try:
        return p.pi.next()
    except StopIteration:
        raise EOFError(label)
