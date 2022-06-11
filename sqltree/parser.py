from dataclasses import dataclass, field, replace
from typing import (
    Callable,
    Generic,
    Iterable,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from sqltree.dialect import Dialect, Feature

from .location import Location
from .peeking_iterator import PeekingIterator
from .tokenizer import Token, TokenType


@dataclass
class EOFError(Exception):
    """Raised when EOF is encountered in the parser. This should never
    happen because it means we parsed past the special EOF token."""

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
        if token.typ is TokenType.eof:
            text = "EOF"
        else:
            text = repr(token.text)
        return ParseError(f"Unexpected {text} (expected {expected})", token.loc)

    @classmethod
    def from_disallowed(
        cls, token: Token, dialect: Dialect, feature: str
    ) -> "ParseError":
        return ParseError(f"{dialect} does not support {feature}", token.loc)

    def __str__(self) -> str:
        return f"{self.message}\n{self.location.display().rstrip()}"


@dataclass
class Parser:
    pi: PeekingIterator[Token]
    dialect: Dialect


@dataclass
class Node:
    pass


NodeT = TypeVar("NodeT", bound=Node, covariant=True)


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
class KeywordIdentifier(Expression):
    keyword: Keyword


@dataclass
class Dotted(Expression):
    left: Identifier
    dot: Punctuation
    right: Identifier


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
class NullExpression(Expression):
    null_kw: Keyword


@dataclass
class FunctionCall(Expression):
    callee: Expression
    left_paren: Punctuation
    args: Sequence[WithTrailingComma[Expression]]
    right_paren: Punctuation


@dataclass
class ExprList(Expression):
    left_paren: Punctuation
    exprs: Sequence[WithTrailingComma[Expression]]
    right_paren: Punctuation


@dataclass
class BinOp(Expression):
    left: Expression
    op: Union[Punctuation, Keyword]
    right: Expression

    def get_precedence(self) -> int:
        op = self.op.text
        return _PRECEDENCE_OF_OP[op]


@dataclass
class Parenthesized(Expression):
    left_punc: Punctuation = field(compare=False, repr=False)
    inner: Expression
    right_punc: Punctuation = field(compare=False, repr=False)


@dataclass
class ElseClause(Node):
    else_kw: Keyword
    expr: Expression


@dataclass
class WhenThen(Node):
    when_kw: Keyword
    condition: Expression
    then_kw: Keyword
    result: Expression


@dataclass
class CaseExpression(Expression):
    case_kw: Keyword
    value: Optional[Expression]
    when_thens: Sequence[WhenThen]
    else_clause: Optional[ElseClause]
    end_kw: Keyword


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
class IndexHint(Node):
    intro_kw: Keyword  # USE/IGNORE_FORCE
    kind_kw: Keyword  # INDEX/KEY
    for_kw: Optional[Keyword]
    for_what: Union[None, Keyword, KeywordSequence]  # JOIN/ORDER BY/GROUP BY
    left_paren: Punctuation
    index_list: Sequence[WithTrailingComma[Identifier]]
    right_paren: Punctuation


@dataclass
class JoinOn(Node):
    kw: Keyword
    search_condition: Expression


@dataclass
class JoinUsing(Node):
    kw: Keyword
    left_paren: Punctuation
    join_column_list: Sequence[WithTrailingComma[Identifier]]
    right_paren: Punctuation


JoinSpecification = Union[JoinOn, JoinUsing]


@dataclass
class SimpleJoinedTable(Node):
    left: "TableReference"
    inner_cross: Optional[Keyword]
    join_kw: Keyword
    right: "TableFactor"
    join_specification: Optional[JoinSpecification] = None


@dataclass
class LeftRightJoinedTable(Node):
    left: "TableReference"
    left_right: Keyword
    outer_kw: Optional[Keyword]
    join_kw: Keyword
    right: "TableReference"
    join_specification: JoinSpecification


@dataclass
class NaturalJoinedTable(Node):
    left: "TableReference"
    natural_kw: Keyword
    left_right: Optional[Keyword]
    inner_outer: Optional[Keyword]
    join_kw: Keyword
    right: "TableFactor"


JoinedTable = Union[SimpleJoinedTable, LeftRightJoinedTable, NaturalJoinedTable]


@dataclass
class SimpleTableFactor(Node):
    table_name: Union[Identifier, Dotted]
    as_kw: Optional[Keyword] = None
    alias: Optional[Identifier] = None
    index_hint_list: Sequence[WithTrailingComma[IndexHint]] = field(
        default_factory=list
    )


@dataclass
class SubqueryFactor(Node):
    lateral_kw: Optional[Keyword]
    table_subquery: "Subselect"
    as_kw: Optional[Keyword]
    alias: Identifier
    left_paren: Optional[Punctuation]
    col_list: Sequence[WithTrailingComma[Identifier]]
    right_paren: Optional[Punctuation]


@dataclass
class TableReferenceList(Node):
    left_paren: Punctuation
    references: "TableReferences"
    right_paren: Punctuation


TableFactor = Union[SimpleTableFactor, SubqueryFactor, TableReferenceList]
TableReference = Union[TableFactor, JoinedTable]
TableReferences = Sequence[WithTrailingComma[TableReference]]  # type: ignore


@dataclass
class FromClause(Node):
    kw: Optional[Keyword] = field(compare=False, repr=False)
    table: TableReferences


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
class PlaceholderClause(Node):
    placeholder: Placeholder


MaybeClause = Union[NodeT, PlaceholderClause, None]


@dataclass
class Statement(Node):
    leading_comments: Sequence[Comment] = field(repr=False)


@dataclass
class Select(Statement):
    with_clause: Optional[WithClause]
    select_kw: Keyword = field(compare=False, repr=False)
    modifiers: Sequence[Keyword]
    select_exprs: Sequence[WithTrailingComma[SelectExpr]]
    from_clause: MaybeClause[FromClause] = None
    where: MaybeClause[WhereClause] = None
    group_by: MaybeClause[GroupByClause] = None
    having: MaybeClause[HavingClause] = None
    order_by: MaybeClause[OrderByClause] = None
    limit: MaybeClause[SelectLimitClause] = None


@dataclass
class UnionEntry(Node):
    union_kw: Keyword = field(compare=False, repr=False)
    all_kw: Optional[Keyword]
    select: "Subselect"


@dataclass
class UnionStatement(Statement):
    first: "Subselect"
    others: Sequence[UnionEntry]
    order_by: MaybeClause[OrderByClause] = None
    limit: MaybeClause[SelectLimitClause] = None


@dataclass
class UsingClause(Node):
    kw: Keyword = field(compare=False, repr=False)
    tables: Sequence[WithTrailingComma[Identifier]]


@dataclass
class Delete(Statement):
    with_clause: Optional[WithClause]
    delete_kw: Keyword = field(compare=False, repr=False)
    from_clause: FromClause
    using_clause: MaybeClause[UsingClause] = None
    where: MaybeClause[WhereClause] = None
    order_by: MaybeClause[OrderByClause] = None
    limit: MaybeClause[LimitClause] = None


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
    table: TableReference
    set_clause: SetClause
    where: MaybeClause[WhereClause] = None
    order_by: MaybeClause[OrderByClause] = None
    limit: MaybeClause[LimitClause] = None


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
class Subselect(Expression):
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
    odku: MaybeClause[OdkuClause] = None


@dataclass
class Replace(Statement):
    replace_kw: Keyword = field(compare=False, repr=False)
    into: IntoClause
    values: InsertValues


@dataclass
class StartTransaction(Statement):
    start_kw: Keyword = field(compare=False, repr=False)
    transaction_kw: Keyword = field(compare=False, repr=False)
    characteristics: Sequence[WithTrailingComma[KeywordSequence]]


@dataclass
class BeginStatement(Statement):
    begin_kw: Keyword = field(compare=False, repr=False)
    work_kw: Optional[Keyword] = None


@dataclass
class ChainClause(Node):
    and_kw: Keyword = field(compare=False, repr=False)
    no_kw: Optional[Keyword]
    chain_kw: Keyword = field(compare=False, repr=False)


@dataclass
class ReleaseClause(Node):
    no_kw: Optional[Keyword]
    release_kw: Keyword = field(compare=False, repr=False)


@dataclass
class CommitStatement(Statement):
    commit_kw: Keyword = field(compare=False, repr=False)
    work_kw: Optional[Keyword] = None
    chain: Optional[ChainClause] = None
    release: Optional[ReleaseClause] = None


@dataclass
class RollbackStatement(Statement):
    rollback_kw: Keyword = field(compare=False, repr=False)
    work_kw: Optional[Keyword] = None
    chain: Optional[ChainClause] = None
    release: Optional[ReleaseClause] = None


@dataclass
class DropTable(Statement):
    drop_kw: Keyword = field(compare=False, repr=False)
    temporary_kw: Optional[Keyword]
    table_kw: Keyword = field(compare=False, repr=False)
    if_exists: Optional[KeywordSequence]
    tables: Sequence[WithTrailingComma[Union[Dotted, Identifier]]]
    tail: Optional[Keyword]


def parse(tokens: Iterable[Token], dialect: Dialect) -> Statement:
    p = Parser(PeekingIterator(list(tokens)), dialect)
    return _parse_statement(p)


def _assert_done(p: Parser) -> None:
    remaining = p.pi.peek_or_raise()
    if remaining.typ is not TokenType.eof:
        raise ParseError.from_unexpected_token(remaining, "EOF")


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
        _assert_done(p)
        return statement
    # In Redshift, INSERT is a soft keyword, so INSERT wouldn't match the previous expression.
    elif first.typ is TokenType.identifier:
        try:
            parser = _VERB_TO_PARSER[first.text.upper()]
        except KeyError:
            raise ParseError(f"Unexpected {first.text!r}", first.loc)
        else:
            statement = parser(p)
        _assert_done(p)
        return statement
    elif first.typ is TokenType.comment:
        p.pi.next()
        comment = Comment(first, first.text)
        statement = _parse_statement(p)
        leading_comments = (comment, *statement.leading_comments)
        return replace(statement, leading_comments=leading_comments)
    elif first.typ is TokenType.punctuation and first.text == "(":
        return _parse_select(p)
    else:
        raise ParseError(f"Unexpected {first.text!r}", first.loc)


def _parse_maybe_clause(
    p: Parser, clause_parser: Callable[[Parser], Optional[NodeT]]
) -> MaybeClause[NodeT]:
    token = p.pi.peek()
    if token is not None and token.typ is TokenType.placeholder:
        p.pi.next()
        return PlaceholderClause(Placeholder(token, token.text))
    return clause_parser(p)


def _parse_join_specification(p: Parser) -> Optional[JoinSpecification]:
    on_kw = _maybe_consume_keyword(p, "ON")
    if on_kw is not None:
        expr = _parse_expression(p)
        return JoinOn(on_kw, expr)
    using_kw = _maybe_consume_keyword(p, "USING")
    if using_kw is not None:
        left_paren = _expect_punctuation(p, "(")
        jcl = _parse_comma_separated(p, _parse_identifier)
        right_paren = _expect_punctuation(p, ")")
        return JoinUsing(using_kw, left_paren, jcl, right_paren)
    return None


def _parse_index_hint(p: Parser) -> Optional[IndexHint]:
    intro_kws = {"USE", "IGNORE", "FORCE"}
    kind_kws = {"INDEX", "KEY"}
    if not any(_next_is_keyword(p, kw) for kw in intro_kws):
        return None
    token = p.pi.next()
    intro_kw = Keyword(token, token.text)
    if not any(_next_is_keyword(p, kw) for kw in kind_kws):
        p.pi.wind_back()
        return None
    token = p.pi.next()
    kind_kw = Keyword(token, token.text)
    for_kw = _maybe_consume_keyword(p, "FOR")
    if for_kw is not None:
        for_what = _maybe_consume_keyword(p, "JOIN")
        if for_what is None:
            for_what = _maybe_consume_keyword_sequence(p, ["ORDER", "BY"])
            if for_what is None:
                for_what = _maybe_consume_keyword_sequence(p, ["GROUP", "BY"])
                if for_what is None:
                    expected = "JOIN, ORDER BY, or GROUP BY"
                    token = _next_or_else(p, expected)
                    raise ParseError.from_unexpected_token(token, expected)
    else:
        for_what = None
    left_paren = _expect_punctuation(p, "(")
    if _next_is_punctuation(p, ")"):
        index_list = []
        right_paren = _expect_punctuation(p, ")")
        if intro_kw.text != "USE":
            raise ParseError(
                f"Index list must be nonempty for {intro_kw.text} {kind_kw.text}",
                right_paren.token.loc,
            )
    else:
        index_list = _parse_comma_separated(p, _parse_identifier)
        right_paren = _expect_punctuation(p, ")")
    return IndexHint(
        intro_kw, kind_kw, for_kw, for_what, left_paren, index_list, right_paren
    )


def _parse_subquery_factor(
    p: Parser, lateral_kw: Optional[Keyword] = None
) -> SubqueryFactor:
    subselect = _parse_subselect(p, require_parens=True)
    as_kw = _maybe_consume_keyword(p, "AS")
    alias = _parse_identifier(p)
    left_paren = _maybe_consume_punctuation(p, "(")
    if left_paren is not None:
        col_list = _parse_comma_separated(p, _parse_identifier)
        right_paren = _expect_punctuation(p, ")")
    else:
        col_list = ()
        right_paren = None
    return SubqueryFactor(
        lateral_kw, subselect, as_kw, alias, left_paren, col_list, right_paren
    )


def _parse_table_name(p: Parser) -> Union[Dotted, Identifier]:
    identifier = _parse_identifier(p)
    dot = _maybe_consume_punctuation(p, ".")
    if dot is not None:
        right = _parse_identifier(p)
        return Dotted(identifier, dot, right)
    return identifier


def _parse_table_factor(p: Parser) -> TableFactor:
    lateral = _maybe_consume_keyword(p, "LATERAL")
    if lateral is not None:
        return _parse_subquery_factor(p, lateral)
    left_paren = _maybe_consume_punctuation(p, "(")
    if left_paren is not None:
        if _next_is_keyword(p, "SELECT") or _next_is_keyword(p, "WITH"):
            p.pi.wind_back()
            return _parse_subquery_factor(p)
        table_refs = _parse_comma_separated(p, _parse_table_reference)
        right_paren = _expect_punctuation(p, ")")
        return TableReferenceList(left_paren, table_refs, right_paren)
    table_name = _parse_table_name(p)
    as_kw = _maybe_consume_keyword(p, "AS")
    alias = _maybe_parse_identifier(p)
    index_hint_list = _parse_comma_separated_allow_empty(p, _parse_index_hint)
    return SimpleTableFactor(table_name, as_kw, alias, index_hint_list)


JOIN_KEYWORDS = {
    "INNER",
    "CROSS",
    "JOIN",
    "STRAIGHT_JOIN",
    "LEFT",
    "RIGHT",
    "OUTER",
    "NATURAL",
}


def _parse_natural_join(p: Parser, left: TableReference) -> NaturalJoinedTable:
    natural_kw = _expect_keyword(p, "NATURAL")
    inner_kw = _maybe_consume_keyword(p, "INNER")
    if inner_kw is not None:
        inner_outer = inner_kw
        left_right = None
    else:
        left_right = _maybe_consume_keyword(p, "LEFT")
        if left_right is None:
            left_right = _maybe_consume_keyword(p, "RIGHT")
        inner_outer = _maybe_consume_keyword(p, "OUTER")
    join_kw = _expect_keyword(p, "JOIN")
    right = _parse_table_factor(p)
    return NaturalJoinedTable(left, natural_kw, left_right, inner_outer, join_kw, right)


def _parse_left_right_join(p: Parser, left: TableReference) -> LeftRightJoinedTable:
    left_right = _maybe_consume_keyword(p, "LEFT")
    if left_right is None:
        left_right = _expect_keyword(p, "RIGHT")
    outer_kw = _maybe_consume_keyword(p, "OUTER")
    join_kw = _expect_keyword(p, "JOIN")
    right = _parse_table_reference(p)
    join_specification = _parse_join_specification(p)
    if join_specification is None:
        _raise_with_expected(p, "ON or USING")
    return LeftRightJoinedTable(
        left, left_right, outer_kw, join_kw, right, join_specification
    )


def _parse_simple_join(p: Parser, left: TableReference) -> SimpleJoinedTable:
    inner_cross = _maybe_consume_keyword(p, "INNER")
    if inner_cross is None:
        inner_cross = _maybe_consume_keyword(p, "CROSS")
    join_kw = _maybe_consume_keyword(p, "STRAIGHT_JOIN")
    if join_kw is None:
        join_kw = _expect_keyword(p, "JOIN")
    elif inner_cross is not None:
        raise ParseError(
            f"Cannot combine {inner_cross.text} with STRAIGHT_JOIN", join_kw.token.loc
        )
    right = _parse_table_factor(p)
    join_specification = _parse_join_specification(p)
    return SimpleJoinedTable(left, inner_cross, join_kw, right, join_specification)


def _parse_table_reference(p: Parser) -> TableReference:
    ref = _parse_table_factor(p)
    while any(_next_is_keyword(p, kw) for kw in JOIN_KEYWORDS):
        if _next_is_keyword(p, "NATURAL"):
            ref = _parse_natural_join(p, ref)
        elif _next_is_keyword(p, "LEFT") or _next_is_keyword(p, "RIGHT"):
            ref = _parse_left_right_join(p, ref)
        else:
            ref = _parse_simple_join(p, ref)
    return ref


def _parse_table_references(p: Parser) -> TableReferences:
    return _parse_comma_separated(p, _parse_table_reference)


def _parse_from_clause(p: Parser, *, require_from: bool = True) -> Optional[FromClause]:
    from_kw = _maybe_consume_keyword(p, "FROM")
    if from_kw is not None:
        table = _parse_table_references(p)
        return FromClause(from_kw, table)
    if not require_from:
        table = _parse_table_references(p)
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
            raise ParseError.from_disallowed(
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
            raise ParseError.from_disallowed(
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
                raise ParseError.from_disallowed(all_kw.token, p.dialect, "LIMIT ALL")
            expr = All(all_kw)
        else:
            expr = _parse_simple_expression(p)
        if _next_is_punctuation(p, ","):
            offset_leaf = _expect_punctuation(p, ",")
            if not p.dialect.supports_feature(Feature.comma_offset):
                raise ParseError.from_disallowed(
                    offset_leaf.token, p.dialect, "LIMIT offset, row_count"
                )
            if isinstance(expr, All):
                raise ParseError.from_disallowed(
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
    where_clause = _parse_maybe_clause(p, _parse_where_clause)
    allow_limit = p.dialect.supports_feature(Feature.update_limit)
    if allow_limit:
        order_by_clause = _parse_maybe_clause(p, _parse_order_by_clause)
        limit_clause = _parse_maybe_clause(p, _parse_limit_clause)
    else:
        order_by_clause = _parse_order_by_clause(p, allowed=False)
        limit_clause = _parse_limit_clause(p, allowed=False)
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
        raise ParseError.from_disallowed(kw.token, p.dialect, "USING")
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
        raise ParseError.from_unexpected_token(token, "FROM")
    allow_using = p.dialect.supports_feature(Feature.delete_using)
    if allow_using:
        using_clause = _parse_maybe_clause(p, _parse_using_clause)
    else:
        using_clause = _parse_using_clause(p, allowed=False)
    where_clause = _parse_maybe_clause(p, _parse_where_clause)
    allow_limit = p.dialect.supports_feature(Feature.update_limit)
    if allow_limit:
        order_by_clause = _parse_maybe_clause(p, _parse_order_by_clause)
        limit_clause = _parse_maybe_clause(p, _parse_limit_clause)
    else:
        order_by_clause = _parse_order_by_clause(p, allowed=False)
        limit_clause = _parse_limit_clause(p, allowed=False)
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


def _parse_select(p: Parser) -> Union[Select, UnionStatement]:
    if _next_is_punctuation(p, "("):
        first = _parse_subselect(p, True)
    else:
        select_stmt = _parse_single_select(p)
        first = Subselect(None, select_stmt, None)
        if not _next_is_keyword(p, "UNION"):
            return select_stmt
    rest = []
    while _next_is_keyword(p, "UNION"):
        union_kw = _expect_keyword(p, "UNION")
        kw = _maybe_consume_one_of_keywords(p, ("ALL", "DISTINCT"))
        next_select = _parse_subselect(p, require_parens=_next_is_punctuation(p, "("))
        rest.append(UnionEntry(union_kw, kw, next_select))
    order_by_clause = _parse_maybe_clause(p, _parse_order_by_clause)
    select_limit_clause = _parse_maybe_clause(p, _parse_select_limit_clause)
    return UnionStatement((), first, rest, order_by_clause, select_limit_clause)


def _parse_single_select(p: Parser) -> Select:
    if p.dialect.supports_feature(Feature.with_clause) and _next_is_keyword(p, "WITH"):
        with_clause = _parse_with_clause(p)
    else:
        with_clause = None
    select = _expect_keyword(p, "SELECT")
    possible_modifiers = p.dialect.get_select_modifiers()
    modifiers = []
    for group in possible_modifiers:
        kw = _maybe_consume_one_of_keywords(p, group)
        if kw is not None:
            modifiers.append(kw)
    select_exprs = _parse_comma_separated(p, _parse_select_expr)

    from_clause = _parse_maybe_clause(p, _parse_from_clause)
    where_clause = _parse_maybe_clause(p, _parse_where_clause)
    group_by_clause = _parse_maybe_clause(p, _parse_group_by_clause)
    having_clause = _parse_maybe_clause(p, _parse_having_clause)
    order_by_clause = _parse_maybe_clause(p, _parse_order_by_clause)
    select_limit_clause = _parse_maybe_clause(p, _parse_select_limit_clause)

    return Select(
        (),
        with_clause,
        select,
        modifiers,
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


def _raise_with_expected(p: Parser, expected: str) -> NoReturn:
    token = p.pi.peek_or_raise()
    raise ParseError.from_unexpected_token(token, expected)


def _parse_comma_separated_allow_empty(
    p: Parser, parse_func: Callable[[Parser], Optional[NodeT]]
) -> Sequence[WithTrailingComma[NodeT]]:
    nodes = []
    while True:
        inner = parse_func(p)
        if inner is None:
            if not nodes:
                return []
            else:
                _raise_with_expected(p, "next list element")
        comma = _maybe_consume_punctuation(p, ",")
        node = WithTrailingComma(inner, comma)
        nodes.append(node)
        if comma is None:
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
    table = _parse_table_name(p)
    col_names = _maybe_parse_col_name_list(p)
    return IntoClause(into, table, col_names)


def _parse_value_list(p: Parser) -> ValueList:
    open_paren = _expect_punctuation(p, "(")
    values = _parse_comma_separated(p, _parse_value)
    close_paren = _expect_punctuation(p, ")")
    return ValueList(open_paren, values, close_paren)


def _parse_subselect(p: Parser, require_parens: bool) -> Subselect:
    if not require_parens:
        select = _parse_single_select(p)
        return Subselect(None, select, None)
    left = _expect_punctuation(p, "(")
    select = _parse_single_select(p)
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
    odku = _parse_maybe_clause(p, _parse_odku_clause)
    return Insert((), insert, ignore, into, values, odku)


def _parse_replace(p: Parser) -> Replace:
    insert = _expect_keyword(p, "REPLACE")
    if not p.dialect.supports_feature(Feature.replace):
        raise ParseError(f"{p.dialect} does not support REPLACE", insert.token.loc)
    into = _parse_into_clause(p)
    values = _parse_values_clause(p)
    return Replace((), insert, into, values)


def _parse_transaction_characteristic(p: Parser) -> Optional[KeywordSequence]:
    sequences = [
        ["WITH", "CONSISTENT", "SNAPSHOT"],
        ["READ", "WRITE"],
        ["READ", "ONLY"],
    ]
    for seq in sequences:
        kws = _maybe_consume_keyword_sequence(p, seq)
        if kws is not None:
            return kws
    return None


def _parse_start(p: Parser) -> StartTransaction:
    start = _expect_keyword(p, "START")
    transaction = _expect_keyword(p, "TRANSACTION")
    chars = _parse_comma_separated_allow_empty(p, _parse_transaction_characteristic)
    return StartTransaction((), start, transaction, chars)


def _parse_begin(p: Parser) -> BeginStatement:
    begin = _expect_keyword(p, "BEGIN")
    work = _maybe_consume_keyword(p, "WORK")
    return BeginStatement((), begin, work)


def _parse_chain_clause(p: Parser) -> Optional[ChainClause]:
    if not _next_is_keyword(p, "AND"):
        return None
    and_ = _expect_keyword(p, "AND")
    no = _maybe_consume_keyword(p, "NO")
    chain = _expect_keyword(p, "CHAIN")
    return ChainClause(and_, no, chain)


def _parse_release_clause(p: Parser) -> Optional[ReleaseClause]:
    if not _next_is_keyword(p, "NO"):
        release_kw = _maybe_consume_keyword(p, "RELEASE")
        if release_kw is not None:
            return ReleaseClause(None, release_kw)
        return None
    no = _expect_keyword(p, "NO")
    release = _expect_keyword(p, "RELEASE")
    return ReleaseClause(no, release)


def _parse_commit(p: Parser) -> CommitStatement:
    commit = _expect_keyword(p, "COMMIT")
    work = _maybe_consume_keyword(p, "WORK")
    chain = _parse_chain_clause(p)
    release = _parse_release_clause(p)
    return CommitStatement((), commit, work, chain, release)


def _parse_rollback(p: Parser) -> RollbackStatement:
    commit = _expect_keyword(p, "ROLLBACK")
    work = _maybe_consume_keyword(p, "WORK")
    chain = _parse_chain_clause(p)
    release = _parse_release_clause(p)
    return RollbackStatement((), commit, work, chain, release)


def _parse_drop(p: Parser) -> DropTable:
    drop = _expect_keyword(p, "DROP")
    temporary = _maybe_consume_keyword(p, "TEMPORARY")
    table = _expect_keyword(p, "TABLE")
    if_exists = _maybe_consume_keyword_sequence(p, ["IF", "EXISTS"])
    tables = _parse_comma_separated(p, _parse_table_name)
    tail = _maybe_consume_one_of_keywords(p, ["CASCADE", "RESTRICT"])
    return DropTable((), drop, temporary, table, if_exists, tables, tail)


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
    token = p.pi.peek_or_raise()
    statement = _parse_statement(p)
    if isinstance(statement, (Select, Update, Delete)):
        return replace(statement, with_clause=with_clause)
    raise ParseError.from_unexpected_token(token, "SELECT, UPDATE, or DELETE")


_VERB_TO_PARSER = {
    "SELECT": _parse_select,
    "UPDATE": _parse_update,
    "DELETE": _parse_delete,
    "INSERT": _parse_insert,
    "REPLACE": _parse_replace,
    "WITH": _parse_with,
    "START": _parse_start,
    "BEGIN": _parse_begin,
    "COMMIT": _parse_commit,
    "ROLLBACK": _parse_rollback,
    "DROP": _parse_drop,
}


def _maybe_parse_identifier(p: Parser) -> Optional[Identifier]:
    token = p.pi.peek()
    if token is None:
        return None
    if token.typ is not TokenType.identifier:
        if token.typ is TokenType.string:
            delimiter = p.dialect.get_identifier_delimiter()
            if token.text[0] == delimiter == token.text[-1]:
                identifier = token.text[1:-1]
                p.pi.next()
                return Identifier(token, identifier)
        return None
    p.pi.next()
    return Identifier(token, token.text)


def _parse_identifier(p: Parser) -> Identifier:
    token = _next_or_else(p, "identifier")
    if token.typ is not TokenType.identifier:
        if token.typ is TokenType.string:
            delimiter = p.dialect.get_identifier_delimiter()
            if token.text[0] == delimiter == token.text[-1]:
                identifier = token.text[1:-1]
                return Identifier(token, identifier)
        raise ParseError.from_unexpected_token(token, "identifier")
    return Identifier(token, token.text)


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
_PRECEDENCE_OF_OP = {
    text: precedence
    for precedence, ops in enumerate(_BINOP_PRECEDENCE)
    for _, text in ops
}
MIN_BOOLEAN_PRECEDENCE = _PRECEDENCE_OF_OP["AND"]


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
            if token.text in ("IN", "NOT IN"):
                right = _parse_in_rhs(p)
            else:
                right = _parse_binop(p, precedence - 1)
            left = BinOp(left, op, right)
        else:
            return left


def _parse_in_rhs(p: Parser) -> Expression:
    token = p.pi.peek()
    if token is not None and token.typ is TokenType.placeholder:
        p.pi.next()
        return Placeholder(token, token.text)
    left = _expect_punctuation(p, "(")
    if _next_is_keyword(p, "SELECT") or _next_is_keyword(p, "WITH"):
        p.pi.wind_back()
        return _parse_subselect(p, True)
    exprs = _parse_comma_separated(p, _parse_expression)
    right = _expect_punctuation(p, ")")
    return ExprList(left, exprs, right)


# Keywords that support function-like syntax
KEYWORD_FUNCTIONS = {"VALUES"}


def _parse_function_call(p: Parser, callee: Expression) -> FunctionCall:
    left_paren = _expect_punctuation(p, "(")
    args = []
    if not _next_is_punctuation(p, ")"):
        args = _parse_comma_separated(p, _parse_expression)
    right_paren = _expect_punctuation(p, ")")
    return FunctionCall(callee, left_paren, args, right_paren)


def _parse_else_clause(p: Parser) -> Optional[ElseClause]:
    kw = _maybe_consume_keyword(p, "ELSE")
    if kw is None:
        return None
    expr = _parse_expression(p)
    return ElseClause(kw, expr)


def _parse_when_then(p: Parser) -> Optional[WhenThen]:
    when_kw = _maybe_consume_keyword(p, "WHEN")
    if when_kw is None:
        return None
    condition = _parse_expression(p)
    then_kw = _expect_keyword(p, "THEN")
    result = _parse_expression(p)
    return WhenThen(when_kw, condition, then_kw, result)


def _parse_case_expression(p: Parser) -> CaseExpression:
    case_kw = _expect_keyword(p, "CASE")
    if _next_is_keyword(p, "WHEN"):
        value = None
    else:
        value = _parse_expression(p)
    when_thens = []
    while True:
        when_then = _parse_when_then(p)
        if when_then is None:
            break
        when_thens.append(when_then)
    else_clause = _parse_else_clause(p)
    end_kw = _expect_keyword(p, "END")
    return CaseExpression(case_kw, value, when_thens, else_clause, end_kw)


def _parse_identifier_expression(p: Parser, identifier: Identifier) -> Expression:
    if _next_is_punctuation(p, "("):
        return _parse_function_call(p, identifier)
    dot = _maybe_consume_punctuation(p, ".")
    if dot is not None:
        right = _parse_identifier(p)
        return Dotted(identifier, dot, right)
    return identifier


def _parse_simple_expression(p: Parser) -> Expression:
    token = _next_or_else(p, "expression")
    if token.typ is TokenType.punctuation and token.text == "*":
        return Star(token)
    elif token.typ is TokenType.punctuation and token.text == "(":
        if _next_is_keyword(p, "SELECT") or _next_is_keyword(p, "WITH"):
            p.pi.wind_back()
            return _parse_subselect(p, True)
        inner = _parse_expression(p)
        right = _expect_punctuation(p, ")")
        return Parenthesized(Punctuation(token, "("), inner, right)
    elif token.typ is TokenType.identifier:
        expr = Identifier(token, token.text)
        return _parse_identifier_expression(p, expr)
    elif token.typ is TokenType.number:
        return IntegerLiteral(token, int(token.text))
    elif token.typ is TokenType.placeholder:
        return Placeholder(token, token.text)
    elif token.typ is TokenType.string:
        text = token.text[1:-1]
        if token.text[0] == p.dialect.get_identifier_delimiter():
            return _parse_identifier_expression(p, Identifier(token, text))
        else:
            return StringLiteral(token, text)
    elif token.typ is TokenType.keyword:
        if token.text in KEYWORD_FUNCTIONS:
            kw = Keyword(token, token.text)
            return _parse_function_call(p, KeywordIdentifier(kw))
        elif token.text == "CASE":
            p.pi.wind_back()
            return _parse_case_expression(p)
        elif token.text == "NULL":
            return NullExpression(Keyword(token, token.text))
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
        raise ParseError.from_unexpected_token(token, repr(keyword))
    return Keyword(token, token.text)


def _maybe_consume_keyword(p: Parser, keyword: str) -> Optional[Keyword]:
    for token in p.pi:
        if _token_is_keyword(p, token, keyword):
            return Keyword(token, token.text)
        else:
            p.pi.wind_back()
            break
    return None


def _maybe_consume_one_of_keywords(
    p: Parser, keywords: Sequence[str]
) -> Optional[Keyword]:
    for token in p.pi:
        for keyword in keywords:
            if _token_is_keyword(p, token, keyword):
                return Keyword(token, token.text)
        p.pi.wind_back()
        break
    return None


def _next_or_else(p: Parser, label: str) -> Token:
    try:
        return p.pi.next()
    except StopIteration:
        raise EOFError(label)
