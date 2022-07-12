from dataclasses import dataclass, field, replace
from typing import (
    Callable,
    Dict,
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


class Clause(Node):
    """Base class for SQL "clauses", like WHERE.

    By default, the formatter puts these on a new line.

    """


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
    right: Union[Identifier, Punctuation]  # can be *


@dataclass
class DottedTable(Node):
    left: Identifier
    dot: Punctuation
    right: Identifier


@dataclass
class SimpleTableName(Node):
    identifier: Identifier


@dataclass
class StringLiteral(Expression, Leaf):
    value: str


@dataclass
class NumericLiteral(Expression, Leaf):
    # We just store the value as a string so we don't
    # have to worry about the exact semantics of different
    # kinds of numbers.
    value: str


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
class CharsetInfo(Node):
    character_kw: Keyword = field(repr=False, compare=False)
    set_kw: Keyword = field(repr=False, compare=False)
    charset: Union[Identifier, Placeholder, StringLiteral]


@dataclass
class CharType(Node):
    call: Union[Keyword, FunctionCall]
    charset: Optional[Union[CharsetInfo, Keyword]] = None


CastType = Union[FunctionCall, CharType, Keyword, KeywordSequence]


@dataclass
class Cast(Expression):
    cast_kw: Identifier = field(repr=False, compare=False)
    left_paren: Punctuation = field(repr=False, compare=False)
    expr: Expression
    as_kw: Keyword = field(repr=False, compare=False)
    type_name: CastType
    array_kw: Optional[Keyword]
    right_paren: Punctuation = field(repr=False, compare=False)


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
class UnaryOp(Expression):
    op: Punctuation
    expr: Expression


@dataclass
class Parenthesized(Expression):
    left_punc: Punctuation = field(compare=False, repr=False)
    inner: Expression
    right_punc: Punctuation = field(compare=False, repr=False)


@dataclass
class Binary(Expression):
    binary_kw: Keyword = field(compare=False, repr=False)
    expr: Expression


@dataclass
class Distinct(Expression):
    distinct_kw: Keyword = field(compare=False, repr=False)
    expr: Expression


@dataclass
class Not(Expression):
    not_kw: Keyword = field(compare=False, repr=False)
    expr: Expression


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
    index_list: Sequence[WithTrailingComma[Union[Identifier, Keyword]]]
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
TableName = Union[SimpleTableName, DottedTable]


@dataclass
class SimpleTableFactor(Node):
    table_name: TableName
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
class FromClause(Clause):
    kw: Optional[Keyword] = field(compare=False, repr=False)
    table: TableReferences


@dataclass
class WhereClause(Clause):
    kw: Keyword = field(compare=False, repr=False)
    conditions: Expression


@dataclass
class GroupByClause(Clause):
    kwseq: KeywordSequence = field(compare=False, repr=False)
    expr: Sequence[WithTrailingComma[OrderByExpr]]


@dataclass
class HavingClause(Clause):
    kw: Keyword = field(compare=False, repr=False)
    conditions: Expression


@dataclass
class OrderByClause(Clause):
    kwseq: KeywordSequence = field(compare=False, repr=False)
    expr: Sequence[WithTrailingComma[OrderByExpr]]


@dataclass
class LimitClause(Clause):
    kw: Keyword = field(compare=False, repr=False)
    row_count: Expression


@dataclass
class All(Node):
    kw: Keyword = field(compare=False, repr=False)


@dataclass
class SelectLimitClause(Clause):
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
class LockMode(Clause):
    mode: KeywordSequence  # FOR UPDATE, FOR SHARE
    modifier: Optional[Union[Keyword, KeywordSequence]]  # NOWAIT, SKIP LOCKED


@dataclass
class PlaceholderClause(Clause):
    placeholder: Placeholder


@dataclass
class SeparatorClause(Node):
    separator_kw: Keyword = field(compare=False, repr=False)
    separator: Union[StringLiteral, Placeholder]


MaybeClause = Union[NodeT, PlaceholderClause, None]


@dataclass
class GroupConcat(Expression):
    group_concat_kw: Identifier = field(repr=False, compare=False)
    left_paren: Punctuation = field(repr=False, compare=False)
    distinct_kw: Optional[Keyword]
    exprs: Sequence[WithTrailingComma[Expression]]
    order_by: MaybeClause[OrderByClause]
    separator: MaybeClause[SeparatorClause]
    right_paren: Punctuation = field(repr=False, compare=False)


@dataclass
class KwSeqString(Node):
    kwseq: KeywordSequence
    string: Union[StringLiteral, Placeholder]


@dataclass
class FieldsOptions(Node):
    fields_kw: Keyword
    terminated_by: Optional[KwSeqString]
    enclosed_by: Optional[KwSeqString]
    escaped_by: Optional[KwSeqString]


@dataclass
class LinesOptions(Node):
    fields_kw: Keyword
    starting_by: Optional[KwSeqString]
    terminated_by: Optional[KwSeqString]


@dataclass
class IntoOutfile(Clause):
    into_kw: Keyword = field(repr=False, compare=False)
    outfile_kw: Keyword = field(repr=False, compare=False)
    filename: Union[StringLiteral, Placeholder]
    charset: Optional[CharsetInfo]
    fields_options: Optional[FieldsOptions]
    lines_options: Optional[LinesOptions]


@dataclass
class IntoDumpfile(Clause):
    into_kw: Keyword = field(repr=False, compare=False)
    dumpfile_kw: Keyword = field(repr=False, compare=False)
    filename: Union[StringLiteral, Placeholder]


# TODO INTO @var_name. Note that "@ var_name" is illegal, this needs
# tokenizer work.
IntoOption = Union[IntoOutfile, IntoDumpfile]


@dataclass
class Statement(Node):
    leading_comments: Sequence[Comment] = field(repr=False)


@dataclass
class Select(Statement):
    with_clause: Optional[WithClause]
    select_kw: Keyword = field(compare=False, repr=False)
    modifiers: Sequence[Keyword]
    select_exprs: Sequence[WithTrailingComma[SelectExpr]]
    into_before_from: MaybeClause[IntoOption] = None
    from_clause: MaybeClause[FromClause] = None
    where: MaybeClause[WhereClause] = None
    group_by: MaybeClause[GroupByClause] = None
    having: MaybeClause[HavingClause] = None
    order_by: MaybeClause[OrderByClause] = None
    limit: MaybeClause[SelectLimitClause] = None
    into_before_lock_mode: MaybeClause[IntoOption] = None
    lock_mode: MaybeClause[LockMode] = None
    into: MaybeClause[IntoOption] = None


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
class UsingClause(Clause):
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
class SetClause(Clause):
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
class IntoClause(Clause):
    kw: Optional[Keyword] = field(compare=False, repr=False)
    table: TableName
    col_names: Optional[ColNameList]


@dataclass
class ValueList(Node):
    open_paren: Punctuation
    values: Sequence[WithTrailingComma[Value]]
    close_paren: Punctuation


@dataclass
class ValuesClause(Clause):
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
class OdkuClause(Clause):
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
class IsolationLevel(Node):
    isolation_kw: Keyword = field(compare=False, repr=False)
    level_kw: Keyword = field(compare=False, repr=False)
    level: KeywordSequence


TransactionCharacteristic = Union[IsolationLevel, KeywordSequence]


@dataclass
class SetTransaction(Statement):
    set_kw: Keyword = field(compare=False, repr=False)
    scope_kw: Optional[Keyword]
    transaction_kw: Keyword = field(compare=False, repr=False)
    characteristics: Sequence[WithTrailingComma[TransactionCharacteristic]]


@dataclass
class DropTable(Statement):
    drop_kw: Keyword = field(compare=False, repr=False)
    temporary_kw: Optional[Keyword]
    table_kw: Keyword = field(compare=False, repr=False)
    if_exists: Optional[KeywordSequence]
    tables: Sequence[WithTrailingComma[TableName]]
    tail: Optional[Keyword]


@dataclass
class LikeTable(Node):
    like_kw: Keyword = field(compare=False, repr=False)
    table: TableName


@dataclass
class ParenthesizedLikeTable(Node):
    left_paren: Punctuation
    like_table: LikeTable
    right_paren: Punctuation


CreateTableDefinition = Union[LikeTable, ParenthesizedLikeTable]


@dataclass
class CreateTable(Statement):
    create_kw: Keyword = field(compare=False, repr=False)
    temporary_kw: Optional[Keyword]
    table_kw: Keyword = field(compare=False, repr=False)
    if_not_exists: Optional[KeywordSequence]
    table_name: TableName
    definition: CreateTableDefinition


@dataclass
class Truncate(Statement):
    truncate_kw: Keyword = field(compare=False, repr=False)
    table_kw: Optional[Keyword]
    table: TableName


@dataclass
class TableTo(Node):
    table: TableName
    to_kw: Keyword = field(compare=False, repr=False)
    new_table: TableName


@dataclass
class RenameTables(Statement):
    rename_kw: Keyword = field(compare=False, repr=False)
    table_kw: Keyword = field(compare=False, repr=False)
    tables: Sequence[WithTrailingComma[TableTo]]


@dataclass
class DatabaseClause(Node):
    kw: Keyword  # FROM or IN
    db_name: Identifier


@dataclass
class LikeClause(Clause):
    like_kw: Keyword = field(compare=False, repr=False)
    pattern: Expression


LikeOrWhere = MaybeClause[Union[LikeClause, WhereClause]]


@dataclass
class ShowColumns(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    extended_kw: Optional[Keyword]
    full_kw: Optional[Keyword]
    columns_kw: Keyword  # COLUMNS or FIELDS
    from_kw: Keyword  # FROM, IN
    table_name: TableName
    db_clause: MaybeClause[DatabaseClause]
    like_clause: LikeOrWhere


@dataclass
class ShowIndex(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    extended_kw: Optional[Keyword]
    index_kw: Keyword  # INDEX, INDEXES, KEYS
    from_kw: Keyword  # FROM, IN
    table_name: TableName
    db_clause: MaybeClause[DatabaseClause]
    where_clause: MaybeClause[WhereClause]


@dataclass
class ShowTables(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    extended_kw: Optional[Keyword]
    full_kw: Optional[Keyword]
    tables_kw: Keyword = field(compare=False, repr=False)
    db_clause: MaybeClause[DatabaseClause]
    like_clause: LikeOrWhere


@dataclass
class ShowTableStatus(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    table_kw: Keyword = field(compare=False, repr=False)
    status_kw: Keyword = field(compare=False, repr=False)
    db_clause: MaybeClause[DatabaseClause]
    like_clause: LikeOrWhere


@dataclass
class ShowTriggers(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    triggers_kw: Keyword = field(compare=False, repr=False)
    db_clause: MaybeClause[DatabaseClause]
    like_clause: LikeOrWhere


@dataclass
class ChannelClause(Node):
    for_kw: Keyword = field(compare=False, repr=False)
    channel_kw: Keyword = field(compare=False, repr=False)
    channel: Expression


@dataclass
class ShowReplicaStatus(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    replica_kw: Keyword  # SLAVE or REPLICA
    status_kw: Keyword = field(compare=False, repr=False)
    channel_clause: Optional[ChannelClause] = None


@dataclass
class ShowReplicas(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    replicas_kw: Union[Keyword, KeywordSequence]  # REPLICAS or SLAVE HOSTS


@dataclass
class ShowStatus(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    modifier: Optional[Keyword]  # GLOBAL or SESSION
    status_kw: Keyword = field(compare=False, repr=False)
    like_clause: LikeOrWhere


@dataclass
class ShowVariables(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    modifier: Optional[Keyword]  # GLOBAL or SESSION
    variables_kw: Keyword = field(compare=False, repr=False)
    like_clause: LikeOrWhere


@dataclass
class ShowWarnErrorCount(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    count_kw: Keyword = field(compare=False, repr=False)
    left_paren: Punctuation
    star: Punctuation
    right_paren: Punctuation
    kind: Keyword  # WARNINGS or ERRORS


@dataclass
class ShowWarnError(Statement):
    show_kw: Keyword = field(compare=False, repr=False)
    kind: Keyword  # WARNINGS or ERRORS
    limit_clause: MaybeClause[SelectLimitClause]


@dataclass
class ExplainType(Node):
    format_kw: Keyword = field(compare=False, repr=False)
    eq: Punctuation = field(compare=False, repr=False)
    format_name: Keyword  # TRADITIONAL, JSON, TREE


@dataclass
class Explain(Statement):
    explain_kw: Keyword
    explain_type: Optional[ExplainType]
    statement: Statement


@dataclass
class RelayLogs(Node):
    relay_kw: Keyword = field(compare=False, repr=False)
    logs_kw: Keyword = field(compare=False, repr=False)
    channel_clause: Optional[ChannelClause] = None


FlushOption = Union[Keyword, KeywordSequence, RelayLogs]


@dataclass
class TablesOption(Node):
    tables_kw: Keyword = field(compare=False, repr=False)
    tables: Sequence[WithTrailingComma[TableName]]
    modifier: Optional[KeywordSequence]


@dataclass
class Flush(Statement):
    flush_kw: Keyword = field(compare=False, repr=False)
    modifier: Optional[Keyword]
    option: Union[Sequence[WithTrailingComma[FlushOption]], TablesOption]


def parse(tokens: Iterable[Token], dialect: Dialect) -> Statement:
    p = Parser(PeekingIterator(list(tokens)), dialect)
    return _parse_statement(p)


def _assert_done(p: Parser, expected: str = "EOF") -> None:
    remaining = p.pi.peek_or_raise()
    if remaining.typ is not TokenType.eof:
        raise ParseError.from_unexpected_token(remaining, expected)


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


def _parse_index(p: Parser) -> Union[Identifier, Keyword]:
    token = _next_or_else(p, "identifier")
    if token.typ is TokenType.identifier:
        return Identifier(token, token.text)
    elif token.typ is TokenType.string:
        delimiter = p.dialect.get_identifier_delimiter()
        if token.text[0] == delimiter == token.text[-1]:
            identifier = token.text[1:-1]
            return Identifier(token, identifier)
    elif _token_is_keyword(p, token, "PRIMARY"):
        return Keyword(token, token.text)
    raise ParseError.from_unexpected_token(token, "identifier")


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
        for_what = _expect_one_of_kwseqs(
            p, [["JOIN"], ["ORDER", "BY"], ["GROUP", "BY"]]
        )
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
        index_list = _parse_comma_separated(p, _parse_index)
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


def _parse_table_name(p: Parser) -> TableName:
    identifier = _parse_identifier(p)
    dot = _maybe_consume_punctuation(p, ".")
    if dot is not None:
        right = _parse_identifier(p)
        return DottedTable(identifier, dot, right)
    return SimpleTableName(identifier)


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


def _parse_lock_mode(p: Parser) -> Optional[LockMode]:
    lock_mode = _maybe_consume_keyword_sequence(p, ["FOR", "UPDATE"])
    if lock_mode is None:
        lock_mode = _maybe_consume_keyword_sequence(p, ["FOR", "SHARE"])
        if lock_mode is None:
            return None
    modifier = _maybe_consume_keyword(p, "NOWAIT")
    if modifier is None:
        modifier = _maybe_consume_keyword_sequence(p, ["SKIP", "LOCKED"])
    return LockMode(lock_mode, modifier)


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


def _parse_kwseq_string(p: Parser, keywords: Sequence[str]) -> Optional[KwSeqString]:
    kwseq = _maybe_consume_keyword_sequence(p, keywords)
    if kwseq is None:
        return None
    s = _parse_string_literal(p)
    return KwSeqString(kwseq, s)


def _parse_fields_options(p: Parser) -> Optional[FieldsOptions]:
    kw = _maybe_consume_one_of_keywords(p, ["FIELDS", "COLUMNS"])
    if kw is None:
        return None
    term = _parse_kwseq_string(p, ["TERMINATED", "BY"])
    enclosed = _parse_kwseq_string(p, ["OPTIONALLY", "ENCLOSED", "BY"])
    if enclosed is None:
        enclosed = _parse_kwseq_string(p, ["ENCLOSED", "BY"])
    escaped = _parse_kwseq_string(p, ["ESCAPED", "BY"])
    return FieldsOptions(kw, term, enclosed, escaped)


def _parse_lines_options(p: Parser) -> Optional[LinesOptions]:
    kw = _maybe_consume_keyword(p, "LINES")
    if kw is None:
        return None
    start = _parse_kwseq_string(p, ["STARTING", "BY"])
    term = _parse_kwseq_string(p, ["TERMINATED", "BY"])
    return LinesOptions(kw, start, term)


def _parse_into_option(p: Parser) -> Optional[IntoOption]:
    into_kw = _maybe_consume_keyword(p, "INTO")
    if into_kw is None:
        return None
    next_kw = _expect_one_of_keywords(p, ["OUTFILE", "DUMPFILE"])
    filename = _parse_string_literal(p)
    if next_kw.text == "DUMPFILE":
        return IntoDumpfile(into_kw, next_kw, filename)
    charset = _parse_char_set_info(p)
    fields_opts = _parse_fields_options(p)
    lines_opts = _parse_lines_options(p)
    return IntoOutfile(into_kw, next_kw, filename, charset, fields_opts, lines_opts)


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

    into1 = _parse_maybe_clause(p, _parse_into_option)
    from_clause = _parse_maybe_clause(p, _parse_from_clause)
    where_clause = _parse_maybe_clause(p, _parse_where_clause)
    group_by_clause = _parse_maybe_clause(p, _parse_group_by_clause)
    having_clause = _parse_maybe_clause(p, _parse_having_clause)
    order_by_clause = _parse_maybe_clause(p, _parse_order_by_clause)
    select_limit_clause = _parse_maybe_clause(p, _parse_select_limit_clause)
    into2 = _parse_maybe_clause(p, _parse_into_option)
    lock_mode = _parse_maybe_clause(p, _parse_lock_mode)
    into3 = _parse_maybe_clause(p, _parse_into_option)

    return Select(
        (),
        with_clause,
        select,
        modifiers,
        select_exprs,
        into1,
        from_clause,
        where_clause,
        group_by_clause,
        having_clause,
        order_by_clause,
        select_limit_clause,
        into2,
        lock_mode,
        into3,
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


def _parse_characteristic(p: Parser) -> TransactionCharacteristic:
    isolation_kw = _maybe_consume_keyword(p, "ISOLATION")
    if isolation_kw is not None:
        level_kw = _expect_keyword(p, "LEVEL")
        level = _expect_one_of_kwseqs(
            p,
            [
                ["REPEATABLE", "READ"],
                ["READ", "UNCOMMITTED"],
                ["READ", "COMMITTED"],
                ["SERIALIZABLE"],
            ],
        )
        return IsolationLevel(isolation_kw, level_kw, level)
    return _expect_one_of_kwseqs(p, [["READ", "WRITE"], ["READ", "ONLY"]])


def _parse_set_transaction(p: Parser) -> SetTransaction:
    set_kw = _expect_keyword(p, "SET")
    scope_kw = _maybe_consume_one_of_keywords(p, ["GLOBAL", "SESSION"])
    transaction_kw = _expect_keyword(p, "TRANSACTION")
    characteristics = _parse_comma_separated(p, _parse_characteristic)
    return SetTransaction((), set_kw, scope_kw, transaction_kw, characteristics)


def _parse_drop(p: Parser) -> DropTable:
    drop = _expect_keyword(p, "DROP")
    temporary = _maybe_consume_keyword(p, "TEMPORARY")
    table = _expect_keyword(p, "TABLE")
    if_exists = _maybe_consume_keyword_sequence(p, ["IF", "EXISTS"])
    tables = _parse_comma_separated(p, _parse_table_name)
    tail = _maybe_consume_one_of_keywords(p, ["CASCADE", "RESTRICT"])
    return DropTable((), drop, temporary, table, if_exists, tables, tail)


def _parse_explain(p: Parser) -> Explain:
    explain = _expect_one_of_keywords(p, ["EXPLAIN", "DESCRIBE", "DESC"])
    format_kw = _maybe_consume_keyword(p, "FORMAT")
    if format_kw is not None:
        eq = _expect_punctuation(p, "=")
        format_name = _expect_one_of_keywords(p, ["JSON", "TREE", "TRADITIONAL"])
        explain_type = ExplainType(format_kw, eq, format_name)
    else:
        explain_type = None
    _expect_one_of_keywords(
        p, ["SELECT", "DELETE", "INSERT", "REPLACE", "UPDATE", "TABLE"]
    )
    p.pi.wind_back()
    stmt = _parse_statement(p)
    return Explain((), explain, explain_type, stmt)


def _parse_string_literal(p: Parser) -> Union[StringLiteral, Placeholder]:
    token = _next_or_else(p, "string literal")
    if token.typ is TokenType.string:
        text = token.text[1:-1]
        if token.text[0] == p.dialect.get_identifier_delimiter():
            raise ParseError.from_unexpected_token(token, "string literal")
        return StringLiteral(token, text)
    elif token.typ is TokenType.placeholder:
        return Placeholder(token, token.text)
    else:
        raise ParseError.from_unexpected_token(token, "string literal")


def _parse_charset(p: Parser) -> Union[StringLiteral, Identifier, Placeholder]:
    token = _next_or_else(p, "string literal")
    if token.typ is TokenType.string:
        text = token.text[1:-1]
        if token.text[0] == p.dialect.get_identifier_delimiter():
            raise ParseError.from_unexpected_token(token, "string literal")
        return StringLiteral(token, text)
    elif token.typ is TokenType.placeholder:
        return Placeholder(token, token.text)
    elif token.typ is TokenType.identifier:
        return Identifier(token, token.text)
    else:
        raise ParseError.from_unexpected_token(token, "string literal")


def _parse_channel_clause(p: Parser) -> Optional[ChannelClause]:
    for_kw = _maybe_consume_keyword(p, "FOR")
    if for_kw is None:
        return None
    channel = _expect_keyword(p, "CHANNEL")
    channel_name = _parse_string_literal(p)
    return ChannelClause(for_kw, channel, channel_name)


def _parse_show_replica_status(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> Union[ShowReplicas, ShowReplicaStatus]:
    _assert_done(modifiers_p, "no modifiers")
    if _token_is_keyword(p, kind_kw.token, "SLAVE"):
        kw = _expect_one_of_keywords(p, ["STATUS", "HOSTS"])
        if _token_is_keyword(p, kw.token, "STATUS"):
            status = kw
        else:
            return ShowReplicas((), show, KeywordSequence([kind_kw, kw]))
    else:
        status = _expect_keyword(p, "STATUS")
    channel_clause = _parse_channel_clause(p)
    return ShowReplicaStatus((), show, kind_kw, status, channel_clause)


def _parse_show_replicas(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowReplicas:
    _assert_done(modifiers_p, "no modifiers")
    return ShowReplicas((), show, kind_kw)


def _parse_like_or_where(p: Parser) -> Optional[LikeOrWhere]:
    like_kw = _maybe_consume_keyword(p, "LIKE")
    if like_kw is not None:
        pattern = _parse_string_literal(p)
        return LikeClause(like_kw, pattern)
    where_kw = _maybe_consume_keyword(p, "WHERE")
    if where_kw is not None:
        expr = _parse_expression(p)
        return WhereClause(where_kw, expr)
    return None


def _parse_db_clause(p: Parser) -> Optional[DatabaseClause]:
    db_kw = _maybe_consume_one_of_keywords(p, ["FROM", "IN"])
    if db_kw is not None:
        db = _parse_identifier(p)
        return DatabaseClause(db_kw, db)
    else:
        return None


def _parse_show_tables(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowTables:
    extended = _maybe_consume_keyword(modifiers_p, "EXTENDED")
    full = _maybe_consume_keyword(modifiers_p, "FULL")
    _assert_done(modifiers_p, "EXTENDED or FULL")
    db_clause = _parse_db_clause(p)
    like_clause = _parse_like_or_where(p)
    return ShowTables((), show, extended, full, kind_kw, db_clause, like_clause)


def _parse_show_columns(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowColumns:
    extended = _maybe_consume_keyword(modifiers_p, "EXTENDED")
    full = _maybe_consume_keyword(modifiers_p, "FULL")
    _assert_done(modifiers_p, "EXTENDED or FULL")
    from_kw = _expect_one_of_keywords(p, ["FROM", "IN"])
    table = _parse_table_name(p)
    db_clause = _parse_db_clause(p)
    like_clause = _parse_like_or_where(p)
    return ShowColumns(
        (), show, extended, full, kind_kw, from_kw, table, db_clause, like_clause
    )


def _parse_show_index(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowIndex:
    extended = _maybe_consume_keyword(modifiers_p, "EXTENDED")
    _assert_done(modifiers_p, "EXTENDED")
    from_kw = _expect_one_of_keywords(p, ["FROM", "IN"])
    table = _parse_table_name(p)
    db_clause = _parse_db_clause(p)
    where_clause = _parse_where_clause(p)
    return ShowIndex(
        (), show, extended, kind_kw, from_kw, table, db_clause, where_clause
    )


def _parse_show_table_status(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowTableStatus:
    _assert_done(modifiers_p, "no modifiers")
    status_kw = _expect_keyword(p, "STATUS")
    db_clause = _parse_db_clause(p)
    like_clause = _parse_like_or_where(p)
    return ShowTableStatus((), show, kind_kw, status_kw, db_clause, like_clause)


def _parse_show_triggers(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowTriggers:
    _assert_done(modifiers_p, "no modifiers")
    db_clause = _parse_db_clause(p)
    like_clause = _parse_like_or_where(p)
    return ShowTriggers((), show, kind_kw, db_clause, like_clause)


def _parse_show_variables(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowVariables:
    modifier = _maybe_consume_one_of_keywords(modifiers_p, ["GLOBAL", "SESSION"])
    _assert_done(modifiers_p, "GLOBAL or SESSION")
    like_clause = _parse_like_or_where(p)
    return ShowVariables((), show, modifier, kind_kw, like_clause)


def _parse_show_status(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowStatus:
    modifier = _maybe_consume_one_of_keywords(modifiers_p, ["GLOBAL", "SESSION"])
    _assert_done(modifiers_p, "GLOBAL or SESSION")
    like_clause = _parse_like_or_where(p)
    return ShowStatus((), show, modifier, kind_kw, like_clause)


def _parse_show_count(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowWarnErrorCount:
    _assert_done(modifiers_p, "no modifiers")
    left_paren = _expect_punctuation(p, "(")
    star = _expect_punctuation(p, "*")
    right_paren = _expect_punctuation(p, ")")
    kind = _expect_one_of_keywords(p, ["ERRORS", "WARNINGS"])
    return ShowWarnErrorCount((), show, kind_kw, left_paren, star, right_paren, kind)


def _parse_show_warnings_or_errors(
    p: Parser, show: Keyword, modifiers_p: Parser, kind_kw: Keyword
) -> ShowWarnError:
    _assert_done(modifiers_p, "no modifiers")
    limit_clause = _parse_select_limit_clause(p)
    return ShowWarnError((), show, kind_kw, limit_clause)


_MODIFIERS = [
    "EXTENDED",  # TABLES
    "FULL",  # TABLES
    "GLOBAL",  # VARIABLES
    "SESSION",  # VARIABLES
]
_ShowParser = Callable[[Parser, Keyword, Parser, Keyword], Statement]
_SHOW_KIND_TO_PARSER: Dict[str, _ShowParser] = {
    "REPLICA": _parse_show_replica_status,
    "SLAVE": _parse_show_replica_status,
    "REPLICAS": _parse_show_replicas,
    "TABLES": _parse_show_tables,
    "TABLE": _parse_show_table_status,
    "TRIGGERS": _parse_show_triggers,
    "VARIABLES": _parse_show_variables,
    "STATUS": _parse_show_status,
    "COUNT": _parse_show_count,
    "WARNINGS": _parse_show_warnings_or_errors,
    "ERRORS": _parse_show_warnings_or_errors,
    "COLUMNS": _parse_show_columns,
    "FIELDS": _parse_show_columns,
    "INDEX": _parse_show_index,
    "INDEXES": _parse_show_index,
    "KEYS": _parse_show_index,
}
_SHOW_KINDS = list(_SHOW_KIND_TO_PARSER)
_EOF = Token(TokenType.eof, "", Location("", 0, 0))


def _parse_show(p: Parser) -> Statement:
    show = _expect_keyword(p, "SHOW")
    modifiers = []
    while True:
        modifier = _maybe_consume_one_of_keywords(p, _MODIFIERS)
        if modifier is None:
            break
        modifiers.append(modifier)
    kind_kw = _expect_one_of_keywords(p, _SHOW_KINDS)
    parser = _SHOW_KIND_TO_PARSER[kind_kw.text.upper()]
    modifiers_parser = Parser(
        PeekingIterator([*[modifier.token for modifier in modifiers], _EOF]), p.dialect
    )
    return parser(p, show, modifiers_parser, kind_kw)


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


def _parse_tables_option(p: Parser) -> TablesOption:
    kw = _expect_keyword(p, "TABLES")
    kwseq = _maybe_consume_keyword_sequence(p, ["WITH", "READ", "LOCK"])
    if kwseq is not None:
        return TablesOption(kw, (), kwseq)
    ident = _maybe_parse_identifier(p)
    if ident is None:
        return TablesOption(kw, (), None)
    p.pi.wind_back()
    tables = _parse_comma_separated(p, _parse_table_name)
    kwseq = _maybe_consume_keyword_sequence(p, ["WITH", "READ", "LOCK"])
    if kwseq is None:
        kwseq = _maybe_consume_keyword_sequence(p, ["FOR", "EXPORT"])
    return TablesOption(kw, tables, kwseq)


def _parse_flush_option(p: Parser) -> FlushOption:
    kw = _expect_one_of_keywords(
        p,
        [
            "BINARY",
            "ENGINE",
            "ERROR",
            "GENERAL",
            "HOSTS",
            "LOGS",
            "PRIVILEGES",
            "OPTIMIZER_COSTS",
            "RELAY",
            "SLOW",
            "STATUS",
            "USER_RESOURCES",
        ],
    )
    kw_text = kw.text.upper()
    if kw_text in ("BINARY", "ENGINE", "ERROR", "GENERAL", "RELAY", "SLOW"):
        logs_kw = _expect_keyword(p, "LOGS")
        if kw_text == "RELAY":
            clause = _parse_channel_clause(p)
            return RelayLogs(kw, logs_kw, clause)
        return KeywordSequence([kw, logs_kw])
    return kw


def _parse_flush(p: Parser) -> Flush:
    kw = _expect_keyword(p, "FLUSH")
    modifier = _maybe_consume_one_of_keywords(p, ["LOCAL", "NO_WRITE_TO_BINLOG"])
    if _next_is_keyword(p, "TABLES"):
        option = _parse_tables_option(p)
    else:
        option = _parse_comma_separated(p, _parse_flush_option)
    return Flush((), kw, modifier, option)


def _parse_truncate(p: Parser) -> Truncate:
    kw = _expect_keyword(p, "TRUNCATE")
    table_kw = _maybe_consume_keyword(p, "TABLE")
    table = _parse_table_name(p)
    return Truncate((), kw, table_kw, table)


def _parse_table_to(p: Parser) -> TableTo:
    old = _parse_table_name(p)
    to = _expect_keyword(p, "TO")
    new = _parse_table_name(p)
    return TableTo(old, to, new)


def _parse_rename(p: Parser) -> RenameTables:
    kw = _expect_keyword(p, "RENAME")
    table_kw = _expect_keyword(p, "TABLE")
    table_to = _parse_comma_separated(p, _parse_table_to)
    return RenameTables((), kw, table_kw, table_to)


def _parse_create(p: Parser) -> CreateTable:
    kw = _expect_keyword(p, "CREATE")
    temporary = _maybe_consume_keyword(p, "TEMPORARY")
    table_kw = _expect_keyword(p, "TABLE")
    kwseq = _maybe_consume_keyword_sequence(p, ["IF", "NOT", "EXISTS"])
    name = _parse_table_name(p)
    # We skip all the other options in the full CREATE TABLE statement. It's
    # quite complicated:
    # https://dev.mysql.com/doc/refman/8.0/en/create-table.html
    like_kw = _maybe_consume_keyword(p, "LIKE")
    if like_kw is not None:
        like_table = _parse_table_name(p)
        defn = LikeTable(like_kw, like_table)
    else:
        left_paren = _expect_punctuation(p, "(")
        like_kw = _expect_keyword(p, "LIKE")
        like_table = _parse_table_name(p)
        right_paren = _expect_punctuation(p, ")")
        defn = ParenthesizedLikeTable(
            left_paren, LikeTable(like_kw, like_table), right_paren
        )
    return CreateTable((), kw, temporary, table_kw, kwseq, name, defn)


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
    "SHOW": _parse_show,
    "EXPLAIN": _parse_explain,
    "DESCRIBE": _parse_explain,
    "DESC": _parse_explain,
    "FLUSH": _parse_flush,
    "TRUNCATE": _parse_truncate,
    "CREATE": _parse_create,
    "RENAME": _parse_rename,
    "SET": _parse_set_transaction,
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
    if token.typ is TokenType.identifier:
        return Identifier(token, token.text)
    elif token.typ is TokenType.string:
        delimiter = p.dialect.get_identifier_delimiter()
        if token.text[0] == delimiter == token.text[-1]:
            identifier = token.text[1:-1]
            return Identifier(token, identifier)
    raise ParseError.from_unexpected_token(token, "identifier")


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
    (P("*"), P("/"), K("DIV"), P("%"), P("%%"), K("MOD")),
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
        star = _maybe_consume_punctuation(p, "*")
        if star is not None:
            right = star
        else:
            right = _parse_identifier(p)
        return Dotted(identifier, dot, right)
    return identifier


def _parse_char_set_info_or_shortcut(
    p: Parser,
) -> Optional[Union[CharsetInfo, Keyword]]:
    simple = _maybe_consume_one_of_keywords(p, ["ASCII", "UNICODE"])
    if simple is not None:
        return simple
    return _parse_char_set_info(p)


def _parse_char_set_info(p: Parser) -> Optional[CharsetInfo]:
    character_kw = _maybe_consume_keyword(p, "CHARACTER")
    if character_kw is None:
        return None
    set_kw = _expect_keyword(p, "SET")
    charset = _parse_charset(p)
    return CharsetInfo(character_kw, set_kw, charset)


def _parse_cast_type(p: Parser) -> CastType:
    kw = _expect_one_of_keywords(
        p,
        [
            "BINARY",
            "CHAR",
            "DATE",
            "DATETIME",
            "DECIMAL",
            "DOUBLE",
            "FLOAT",
            "JSON",
            "NCHAR",
            "REAL",
            "SIGNED",
            "TIME",
            "UNSIGNED",
            "YEAR",
        ],
    )
    if kw.text in ("DATE", "DOUBLE", "JSON", "REAL", "YEAR"):
        return kw
    elif kw.text in ("SIGNED", "UNSIGNED"):
        int_kw = _maybe_consume_keyword(p, "INTEGER")
        if int_kw is None:
            return kw
        else:
            return KeywordSequence([kw, int_kw])
    elif _next_is_punctuation(p, "("):
        call = _parse_function_call(p, KeywordIdentifier(kw))
        if kw.text == "CHAR":
            charset = _parse_char_set_info_or_shortcut(p)
            return CharType(call, charset)
        else:
            return call
    elif kw.text == "CHAR":
        charset = _parse_char_set_info_or_shortcut(p)
        return CharType(kw, charset)
    else:
        return kw


def _parse_cast(cast_kw: Identifier, p: Parser) -> Cast:
    left_paren = _expect_punctuation(p, "(")
    expr = _parse_expression(p)
    as_kw = _expect_keyword(p, "AS")
    cast_type = _parse_cast_type(p)
    array_kw = _maybe_consume_keyword(p, "ARRAY")
    right_paren = _expect_punctuation(p, ")")
    return Cast(cast_kw, left_paren, expr, as_kw, cast_type, array_kw, right_paren)


def _parse_separator_clause(p: Parser) -> Optional[SeparatorClause]:
    kw = _maybe_consume_keyword(p, "SEPARATOR")
    if kw is None:
        return None
    sep = _parse_string_literal(p)
    return SeparatorClause(kw, sep)


def _parse_group_concat(group_concat_kw: Identifier, p: Parser) -> GroupConcat:
    left_paren = _expect_punctuation(p, "(")
    distinct_kw = _maybe_consume_keyword(p, "DISTINCT")
    exprs = _parse_comma_separated(p, _parse_expression)
    order_by = _parse_maybe_clause(p, _parse_order_by_clause)
    separator = _parse_maybe_clause(p, _parse_separator_clause)
    right_paren = _expect_punctuation(p, ")")
    return GroupConcat(
        group_concat_kw,
        left_paren,
        distinct_kw,
        exprs,
        order_by,
        separator,
        right_paren,
    )


def _parse_simple_expression(p: Parser) -> Expression:
    # https://dev.mysql.com/doc/refman/8.0/en/expressions.html
    token = _next_or_else(p, "expression")
    if token.typ is TokenType.punctuation:
        if token.text == "*":
            return Star(token)
        elif token.text == "(":
            if _next_is_keyword(p, "SELECT") or _next_is_keyword(p, "WITH"):
                p.pi.wind_back()
                return _parse_subselect(p, True)
            inner = _parse_expression(p)
            right = _expect_punctuation(p, ")")
            return Parenthesized(Punctuation(token, "("), inner, right)
        elif token.text in ("-", "~"):
            op = Punctuation(token, token.text)
            operand = _parse_simple_expression(p)
            return UnaryOp(op, operand)
    elif token.typ is TokenType.identifier:
        keyword_text = token.text.upper()
        if keyword_text == "CAST":
            return _parse_cast(Identifier(token, keyword_text), p)
        elif keyword_text == "GROUP_CONCAT":
            return _parse_group_concat(Identifier(token, keyword_text), p)
        expr = Identifier(token, token.text)
        return _parse_identifier_expression(p, expr)
    elif token.typ is TokenType.number:
        return NumericLiteral(token, token.text)
    elif token.typ is TokenType.placeholder:
        return Placeholder(token, token.text)
    elif token.typ is TokenType.string:
        text = token.text[1:-1]
        if token.text[0] == p.dialect.get_identifier_delimiter():
            return _parse_identifier_expression(p, Identifier(token, text))
        else:
            return StringLiteral(token, text)
    elif token.typ is TokenType.keyword:
        if token.text == "CASE":
            p.pi.wind_back()
            return _parse_case_expression(p)
        elif token.text == "NULL":
            return NullExpression(Keyword(token, token.text))
        elif token.text == "BINARY":
            expr = _parse_simple_expression(p)
            return Binary(Keyword(token, token.text), expr)
        elif token.text == "DISTINCT":
            expr = _parse_simple_expression(p)
            return Distinct(Keyword(token, token.text), expr)
        elif token.text == "NOT":
            expr = _parse_simple_expression(p)
            return Not(Keyword(token, token.text), expr)
        elif _next_is_punctuation(p, "("):
            kw = Keyword(token, token.text)
            return _parse_function_call(p, KeywordIdentifier(kw))
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


def _expect_one_of_kwseqs(p: Parser, seqs: Sequence[Sequence[str]]) -> KeywordSequence:
    for seq in seqs:
        kwseq = _maybe_consume_keyword_sequence(p, seq)
        if kwseq is not None:
            return kwseq
    raise ParseError.from_unexpected_token(
        p.pi.next(), "one of " + ", ".join(" ".join(seq) for seq in seqs)
    )


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


def _expect_one_of_keywords(p: Parser, keywords: Sequence[str]) -> Keyword:
    expected = "one of " + ", ".join(keywords)
    token = _next_or_else(p, expected)
    for keyword in keywords:
        if _token_is_keyword(p, token, keyword):
            return Keyword(token, token.text.upper())
    raise ParseError.from_unexpected_token(token, expected)


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
