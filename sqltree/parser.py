from dataclasses import dataclass, field, replace
from typing import (Callable, Generic, Iterable, Optional, Sequence, Tuple,
                    TypeVar, Union)

from sqltree.dialect import Dialect, Feature

from .location import Location
from .peeking_iterator import PeekingIterator
from .tokenizer import Token, TokenType


class ParseError(Exception):
    pass


@dataclass
class EOFError(ParseError):
    expected: str

    def __post_init__(self) -> None:
        super().__init__(str(self))

    def __str__(self) -> str:
        return f"Got EOF (expecting {self.expected!r})"


@dataclass
class InvalidSyntax(ParseError):
    message: str
    location: Location

    def __post_init__(self) -> None:
        super().__init__(self.message)
        self.location.display()

    @classmethod
    def from_unexpected_token(cls, token: Token, expected: str) -> "InvalidSyntax":
        return InvalidSyntax(
            f"Unexpected {token.text!r} (expected {expected})", token.loc
        )

    @classmethod
    def from_disallowed(
        cls, token: Token, dialect: Dialect, feature: str
    ) -> "InvalidSyntax":
        return InvalidSyntax(f"{dialect} does not support {feature}", token.loc)

    def __str__(self) -> str:
        return f"{self.message}\n{self.location.display().rstrip()}"


@dataclass
class Parser:
    pi: PeekingIterator[Token]
    dialect: Dialect


@dataclass
class Node:
    pass


NodeT = TypeVar("NodeT", bound=Node)


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
class WithTrailingComma(Node, Generic[NodeT]):
    node: NodeT
    trailing_comma: Optional[Punctuation] = None


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
class FunctionCall(Expression):
    callee: Expression
    left_paren: Punctuation
    args: Sequence[WithTrailingComma[Expression]]
    right_paren: Punctuation


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


@dataclass
class OrderByExpr(Node):
    """Also used for GROUP BY."""

    expr: Expression
    direction_kw: Optional[Keyword] = field(compare=False, repr=False)


@dataclass
class Comment(Leaf):
    text: str


@dataclass
class FromClause(Node):
    kw: Optional[Keyword] = field(compare=False, repr=False)
    table: Expression


@dataclass
class WhereClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    conditions: Expression


@dataclass
class GroupByClause(Node):
    kwseq: KeywordSequence = field(compare=False, repr=False)
    expr: Sequence[WithTrailingComma[OrderByExpr]]


@dataclass
class HavingClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    conditions: Expression


@dataclass
class OrderByClause(Node):
    kwseq: KeywordSequence = field(compare=False, repr=False)
    expr: Sequence[WithTrailingComma[OrderByExpr]]


@dataclass
class LimitClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    row_count: Expression


@dataclass
class All(Node):
    kw: Keyword = field(compare=False, repr=False)


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
    row_count: Union[Expression, All]
    offset: Optional[Expression] = None
    offset_leaf: Union[Punctuation, Keyword, None] = None


@dataclass
class CommonTableExpression(Node):
    table_name: Identifier
    col_names: Optional["ColNameList"]
    as_kw: Keyword = field(compare=False, repr=False)
    subquery: "Subselect"


@dataclass
class WithClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    recursive_kw: Optional[Keyword]
    ctes: Sequence[WithTrailingComma[CommonTableExpression]]


@dataclass
class Statement(Node):
    leading_comments: Sequence[Comment] = field(repr=False)


@dataclass
class Select(Statement):
    with_clause: Optional[WithClause]
    select_kw: Keyword = field(compare=False, repr=False)
    select_exprs: Sequence[WithTrailingComma[SelectExpr]]
    from_clause: Optional[FromClause] = None
    where: Optional[WhereClause] = None
    group_by: Optional[GroupByClause] = None
    having: Optional[HavingClause] = None
    order_by: Optional[OrderByClause] = None
    limit: Optional[SelectLimitClause] = None


@dataclass
class UsingClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    tables: Sequence[WithTrailingComma[Identifier]]


@dataclass
class Delete(Statement):
    with_clause: Optional[WithClause]
    delete_kw: Keyword = field(compare=False, repr=False)
    from_clause: FromClause
    using_clause: Optional[UsingClause] = None
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


@dataclass
class SetClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    assignments: Sequence[WithTrailingComma[Assignment]]


@dataclass
class Update(Statement):
    with_clause: Optional[WithClause]
    update_kw: Keyword = field(compare=False, repr=False)
    table: Expression
    set_clause: SetClause
    where: Optional[WhereClause] = None
    order_by: Optional[OrderByClause] = None
    limit: Optional[LimitClause] = None


@dataclass
class ColNameList(Node):
    open_paren: Punctuation
    col_names: Sequence[WithTrailingComma[Identifier]]
    close_paren: Punctuation


@dataclass
class IntoClause(Node):
    kw: Optional[Keyword] = field(compare=False, repr=False)
    table: Expression
    col_names: Optional[ColNameList]


@dataclass
class ValueList(Node):
    open_paren: Punctuation
    values: Sequence[WithTrailingComma[Value]]
    close_paren: Punctuation


@dataclass
class ValuesClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    value_lists: Sequence[WithTrailingComma[ValueList]]


@dataclass
class DefaultValues(Node):
    kwseq: KeywordSequence = field(compare=False, repr=False)


@dataclass
class Subselect(Node):
    left_paren: Optional[Punctuation]
    select: Select
    right_paren: Optional[Punctuation]


InsertValues = Union[ValuesClause, DefaultValues, Subselect]


@dataclass
class OdkuClause(Node):
    kwseq: KeywordSequence
    assignments: Sequence[WithTrailingComma[Assignment]]


@dataclass
class Insert(Statement):
    insert_kw: Keyword = field(compare=False, repr=False)
    ignore_kw: Optional[Keyword]
    into: IntoClause
    values: InsertValues
    odku: Optional[OdkuClause] = None


@dataclass
class Replace(Statement):
    replace_kw: Keyword = field(compare=False, repr=False)
    into: IntoClause
    values: InsertValues


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
            raise InvalidSyntax(f"Unexpected {first.text!r}", first.loc)
        else:
            statement = parser(p)
        remaining = p.pi.peek()
        if remaining is not None:
            raise InvalidSyntax.from_unexpected_token(remaining, "EOF")
        return statement
    # In Redshift, INSERT is a soft keyword, so INSERT wouldn't match the previous expression.
    elif first.typ is TokenType.identifier:
        try:
            parser = _VERB_TO_PARSER[first.text.upper()]
        except KeyError:
            raise InvalidSyntax(f"Unexpected {first.text!r}", first.loc)
        else:
            statement = parser(p)
        remaining = p.pi.peek()
        if remaining is not None:
            raise InvalidSyntax.from_unexpected_token(remaining, "EOF")
        return statement
    elif first.typ is TokenType.comment:
        p.pi.next()
        comment = Comment(first, first.text)
        statement = _parse_statement(p)
        leading_comments = (comment, *statement.leading_comments)
        return replace(statement, leading_comments=leading_comments)
    else:
        raise InvalidSyntax(f"Unexpected {first.text!r}", first.loc)


def _parse_from_clause(p: Parser, *, require_from: bool = True) -> Optional[FromClause]:
    from_kw = _maybe_consume_keyword(p, "FROM")
    if from_kw is not None:
        table = _parse_table_reference(p)
        return FromClause(from_kw, table)
    if not require_from:
        table = _parse_table_reference(p)
        return FromClause(None, table)
    return None


def _parse_where_clause(p: Parser) -> Optional[WhereClause]:
    kw = _maybe_consume_keyword(p, "WHERE")
    if kw is not None:
        expr = _parse_expression(p)
        return WhereClause(kw, expr)
    return None


def _parse_having_clause(p: Parser) -> Optional[HavingClause]:
    kw = _maybe_consume_keyword(p, "HAVING")
    if kw is not None:
        expr = _parse_expression(p)
        return HavingClause(kw, expr)
    return None


def _parse_group_by_clause(p: Parser) -> Optional[GroupByClause]:
    kwseq = _maybe_consume_keyword_sequence(p, ["GROUP", "BY"])
    if kwseq is not None:
        exprs = _parse_order_by_list(p)
        return GroupByClause(kwseq, exprs)
    else:
        return None


def _parse_order_by_clause(
    p: Parser, *, allowed: bool = True
) -> Optional[OrderByClause]:
    kwseq = _maybe_consume_keyword_sequence(p, ["ORDER", "BY"])
    if kwseq is not None:
        if not allowed:
            raise InvalidSyntax.from_disallowed(
                kwseq.keywords[0].token, p.dialect, "ORDER BY in this context"
            )
        exprs = _parse_order_by_list(p)
        return OrderByClause(kwseq, exprs)
    else:
        return None


def _parse_assignment_list(p: Parser) -> Sequence[WithTrailingComma[Assignment]]:
    return _parse_comma_separated(p, _parse_assignment)


def _parse_set_clause(p: Parser) -> SetClause:
    kw = _expect_keyword(p, "SET")
    assignments = _parse_assignment_list(p)
    return SetClause(kw, assignments)


def _parse_limit_clause(p: Parser, *, allowed: bool = True) -> Optional[LimitClause]:
    kw = _maybe_consume_keyword(p, "LIMIT")
    if kw is not None:
        if not allowed:
            raise InvalidSyntax.from_disallowed(
                kw.token, p.dialect, "LIMIT in this context"
            )
        expr = _parse_simple_expression(p)
        return LimitClause(kw, expr)
    return None


def _parse_select_limit_clause(p: Parser) -> Optional[SelectLimitClause]:
    kw = _maybe_consume_keyword(p, "LIMIT")
    if kw is not None:
        all_kw = _maybe_consume_keyword(p, "ALL")
        if all_kw is not None:
            if not p.dialect.supports_feature(Feature.limit_all):
                raise InvalidSyntax.from_disallowed(
                    all_kw.token, p.dialect, "LIMIT ALL"
                )
            expr = All(all_kw)
        else:
            expr = _parse_simple_expression(p)
        if _next_is_punctuation(p, ","):
            offset_leaf = _expect_punctuation(p, ",")
            if not p.dialect.supports_feature(Feature.comma_offset):
                raise InvalidSyntax.from_disallowed(
                    offset_leaf.token, p.dialect, "LIMIT offset, row_count"
                )
            if isinstance(expr, All):
                raise InvalidSyntax.from_disallowed(
                    offset_leaf.token, p.dialect, "ALL combined with offset"
                )
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
    table = _parse_table_reference(p)
    set_clause = _parse_set_clause(p)
    where_clause = _parse_where_clause(p)
    allow_limit = p.dialect.supports_feature(Feature.update_limit)
    order_by_clause = _parse_order_by_clause(p, allowed=allow_limit)
    limit_clause = _parse_limit_clause(p, allowed=allow_limit)
    return Update(
        (), None, kw, table, set_clause, where_clause, order_by_clause, limit_clause
    )


def _parse_table_name_with_comma(p: Parser) -> WithTrailingComma:
    name = _parse_identifier(p)
    comma = _maybe_consume_punctuation(p, ",")
    return WithTrailingComma(name, comma)


def _parse_using_clause(p: Parser, *, allowed: bool = True) -> Optional[UsingClause]:
    kw = _maybe_consume_keyword(p, "USING")
    if kw is None:
        return None
    if not allowed:
        raise InvalidSyntax.from_disallowed(kw.token, p.dialect, "USING")
    tables = []
    while True:
        table = _parse_table_name_with_comma(p)
        tables.append(table)
        if table.trailing_comma is None:
            break
    return UsingClause(kw, tables)


def _parse_delete(p: Parser) -> Delete:
    delete = _expect_keyword(p, "DELETE")
    from_clause = _parse_from_clause(
        p, require_from=p.dialect.supports_feature(Feature.require_from_for_delete)
    )
    if from_clause is None:
        token = _next_or_else(p, "FROM")
        raise InvalidSyntax.from_unexpected_token(token, "FROM")
    allow_using = p.dialect.supports_feature(Feature.delete_using)
    using_clause = _parse_using_clause(p, allowed=allow_using)
    where_clause = _parse_where_clause(p)
    allow_limit = p.dialect.supports_feature(Feature.update_limit)
    order_by_clause = _parse_order_by_clause(p, allowed=allow_limit)
    limit_clause = _parse_limit_clause(p, allowed=allow_limit)
    return Delete(
        (),
        None,
        delete,
        from_clause,
        using_clause,
        where_clause,
        order_by_clause,
        limit_clause,
    )


def _parse_select(p: Parser) -> Select:
    if p.dialect.supports_feature(Feature.with_clause) and _next_is_keyword(p, "WITH"):
        with_clause = _parse_with_clause(p)
    else:
        with_clause = None
    select = _expect_keyword(p, "SELECT")
    select_exprs = _parse_comma_separated(p, _parse_select_expr)

    from_clause = _parse_from_clause(p)
    where_clause = _parse_where_clause(p)
    group_by_clause = _parse_group_by_clause(p)
    having_clause = _parse_having_clause(p)
    order_by_clause = _parse_order_by_clause(p)
    select_limit_clause = _parse_select_limit_clause(p)

    return Select(
        (),
        with_clause,
        select,
        select_exprs,
        from_clause,
        where_clause,
        group_by_clause,
        having_clause,
        order_by_clause,
        select_limit_clause,
    )


def _parse_with_trailing_comma(
    p: Parser, parse_func: Callable[[Parser], NodeT]
) -> WithTrailingComma[NodeT]:
    name = parse_func(p)
    comma = _maybe_consume_punctuation(p, ",")
    return WithTrailingComma(name, comma)


def _parse_comma_separated(
    p: Parser, parse_func: Callable[[Parser], NodeT]
) -> Sequence[WithTrailingComma[NodeT]]:
    nodes = []
    while True:
        node = _parse_with_trailing_comma(p, parse_func)
        nodes.append(node)
        if node.trailing_comma is None:
            break
    return nodes


def _maybe_parse_col_name_list(p: Parser) -> Optional[ColNameList]:
    if _next_is_punctuation(p, "("):
        open_paren = _expect_punctuation(p, "(")
        col_names = _parse_comma_separated(p, _parse_identifier)
        close_paren = _expect_punctuation(p, ")")
        return ColNameList(open_paren, col_names, close_paren)
    else:
        return None


def _parse_into_clause(p: Parser) -> IntoClause:
    if p.dialect.supports_feature(Feature.require_into_for_ignore):
        into = _expect_keyword(p, "INTO")
    else:
        into = _maybe_consume_keyword(p, "INTO")
    table = _parse_table_reference(p)
    col_names = _maybe_parse_col_name_list(p)
    return IntoClause(into, table, col_names)


def _parse_value_list(p: Parser) -> ValueList:
    open_paren = _expect_punctuation(p, "(")
    values = _parse_comma_separated(p, _parse_value)
    close_paren = _expect_punctuation(p, ")")
    return ValueList(open_paren, values, close_paren)


def _parse_subselect(p: Parser, require_parens: bool) -> Subselect:
    if not require_parens:
        select = _parse_select(p)
        return Subselect(None, select, None)
    left = _expect_punctuation(p, "(")
    select = _parse_select(p)
    right = _expect_punctuation(p, ")")
    return Subselect(left, select, right)


def _parse_values_clause(p: Parser) -> InsertValues:
    if p.dialect.supports_feature(Feature.default_values_on_insert):
        kwseq = _maybe_consume_keyword_sequence(p, ["DEFAULT", "VALUES"])
        if kwseq is not None:
            return DefaultValues(kwseq)
    if p.dialect.supports_feature(Feature.support_value_for_insert):
        kw = _maybe_consume_keyword(p, "VALUE")
    else:
        kw = None
    if kw is None:
        kw = _maybe_consume_keyword(p, "VALUES")
        if kw is None:
            return _parse_subselect(
                p,
                require_parens=p.dialect.supports_feature(
                    Feature.insert_select_require_parens
                ),
            )
    value_lists = _parse_comma_separated(p, _parse_value_list)
    return ValuesClause(kw, value_lists)


def _parse_odku_clause(p: Parser) -> Optional[OdkuClause]:
    odku = _maybe_consume_keyword_sequence(p, ["ON", "DUPLICATE", "KEY", "UPDATE"])
    if odku is None:
        return None
    assignments = _parse_assignment_list(p)
    return OdkuClause(odku, assignments)


def _parse_insert(p: Parser) -> Insert:
    insert = _expect_keyword(p, "INSERT")
    if p.dialect.supports_feature(Feature.insert_ignore):
        ignore = _maybe_consume_keyword(p, "IGNORE")
    else:
        ignore = None
    into = _parse_into_clause(p)
    values = _parse_values_clause(p)
    odku = _parse_odku_clause(p)
    return Insert((), insert, ignore, into, values, odku)


def _parse_replace(p: Parser) -> Replace:
    insert = _expect_keyword(p, "REPLACE")
    if not p.dialect.supports_feature(Feature.replace):
        raise InvalidSyntax(f"{p.dialect} does not support REPLACE", insert.token.loc)
    into = _parse_into_clause(p)
    values = _parse_values_clause(p)
    return Replace((), insert, into, values)


def _parse_cte(p: Parser) -> CommonTableExpression:
    name = _parse_identifier(p)
    col_names = _maybe_parse_col_name_list(p)
    as_kw = _expect_keyword(p, "AS")
    subquery = _parse_subselect(p, require_parens=True)
    return CommonTableExpression(name, col_names, as_kw, subquery)


def _parse_with_clause(p: Parser) -> WithClause:
    kw = _expect_keyword(p, "WITH")
    recursive_kw = _maybe_consume_keyword(p, "RECURSIVE")
    ctes = _parse_comma_separated(p, _parse_cte)
    return WithClause(kw, recursive_kw, ctes)


def _parse_with(p: Parser) -> Statement:
    with_clause = _parse_with_clause(p)
    token = p.pi.peek()
    statement = _parse_statement(p)
    if isinstance(statement, (Select, Update, Delete)):
        return replace(statement, with_clause=with_clause)
    if token is None:
        raise EOFError("SELECT, UPDATE, or DELETE")
    raise InvalidSyntax.from_unexpected_token(token, "SELECT, UPDATE, or DELETE")


_VERB_TO_PARSER = {
    "SELECT": _parse_select,
    "UPDATE": _parse_update,
    "DELETE": _parse_delete,
    "INSERT": _parse_insert,
    "REPLACE": _parse_replace,
    "WITH": _parse_with,
}


def _parse_identifier(p: Parser) -> Identifier:
    token = _next_or_else(p, "identifier")
    if token.typ is not TokenType.identifier:
        raise InvalidSyntax.from_unexpected_token(token, "identifier")
    return Identifier(token, token.text)


def _parse_table_reference(p: Parser) -> Expression:
    # TODO support namespace.table
    return _parse_identifier(p)


def _parse_select_expr(p: Parser) -> SelectExpr:
    expr = _parse_expression(p)
    alias = None
    as_kw = _maybe_consume_keyword(p, "AS")
    if as_kw is not None:
        alias = _parse_identifier(p)
    return SelectExpr(expr, as_kw, alias)


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
    return Assignment(colname, punc, value)


def _parse_order_by_list(p: Parser) -> Sequence[WithTrailingComma[OrderByExpr]]:
    return _parse_comma_separated(p, _parse_order_by_expr)


def _parse_order_by_expr(p: Parser) -> OrderByExpr:
    expr = _parse_expression(p)
    direction = _maybe_consume_keyword(p, "ASC")
    if direction is None:
        direction = _maybe_consume_keyword(p, "DESC")
    return OrderByExpr(expr, direction)


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
        expr = Identifier(token, token.text)
        left_paren = _maybe_consume_punctuation(p, "(")
        if left_paren:
            args = []
            if not _next_is_punctuation(p, ")"):
                while True:
                    arg = _parse_expression(p)
                    comma = _maybe_consume_punctuation(p, ",")
                    args.append(WithTrailingComma(arg, comma))
                    if comma is None:
                        break
            right_paren = _expect_punctuation(p, ")")
            return FunctionCall(expr, left_paren, args, right_paren)
        return expr
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
        raise InvalidSyntax.from_unexpected_token(token, "expression")


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
        raise InvalidSyntax.from_unexpected_token(token, repr(punctuation))
    return Punctuation(token, token.text)


def _maybe_consume_keyword_sequence(
    p: Parser, keywords: Sequence[str]
) -> Optional[KeywordSequence]:
    keywords_found = []
    consumed = 0
    for token in p.pi:
        consumed += 1
        expected = keywords[len(keywords_found)]
        if _token_is_keyword(p, token, expected):
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


def _token_is_keyword(p: Parser, token: Optional[Token], keyword: str) -> bool:
    if token is None:
        return False
    is_hard = keyword in p.dialect.get_keywords()
    if is_hard:
        return token.typ is TokenType.keyword and token.text == keyword
    else:
        return token.typ is TokenType.identifier and token.text.upper() == keyword


def _next_is_keyword(p: Parser, keyword: str) -> bool:
    token = p.pi.peek()
    return _token_is_keyword(p, token, keyword)


def _expect_keyword(p: Parser, keyword: str) -> Keyword:
    token = _next_or_else(p, keyword)
    if not _token_is_keyword(p, token, keyword):
        raise InvalidSyntax.from_unexpected_token(token, repr(keyword))
    return Keyword(token, token.text)


def _maybe_consume_keyword(p: Parser, keyword: str) -> Optional[Keyword]:
    for token in p.pi:
        if _token_is_keyword(p, token, keyword):
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
