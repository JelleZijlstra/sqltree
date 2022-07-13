import argparse
import collections.abc
import sys
import typing
from contextlib import contextmanager
from dataclasses import Field, dataclass, field, fields
from typing import (
    Any,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from . import parser as p
from .api import sqltree
from .dialect import DEFAULT_DIALECT, Dialect
from .tokenizer import Token
from .visitor import Transformer, Visitor

DEFAULT_LINE_LENGTH = 88  # like black
INDENT_SIZE = 4

NoneType = type(None)


class LineTooLong(Exception):
    """Raised internally when a line is about to get too long."""


State = Tuple[int, int, int]


@dataclass
class Formatter(Visitor[None]):
    dialect: Dialect
    line_length: int = DEFAULT_LINE_LENGTH
    indent: int = 0
    lines: List[List[str]] = field(default_factory=list)
    should_skip_comments: bool = False
    current_line_length: int = 0
    can_split: bool = False
    line_has_content: bool = False
    node_stack: List[p.Node] = field(default_factory=list)

    def format(self, tree: p.Node) -> str:
        self.visit(tree)
        sql = "".join(piece for line in self.lines for piece in line)
        if self.indent > 0:
            return f"\n{sql}\n{' ' * (self.indent - INDENT_SIZE)}"
        else:
            return sql + "\n"

    @contextmanager
    def add_indent(self) -> Iterator[None]:
        self.indent += INDENT_SIZE
        try:
            yield
        finally:
            self.indent -= INDENT_SIZE

    @contextmanager
    def override_can_split(self) -> Iterator[State]:
        previous_val = self.can_split
        self.can_split = True
        try:
            yield self.get_state()
        finally:
            self.can_split = previous_val

    def get_state(self) -> State:
        num_pieces = len(self.lines[-1]) if self.lines else 0
        return (len(self.lines), num_pieces, self.current_line_length)

    def restore_state(self, state: State) -> None:
        num_lines, num_pieces, current_line_length = state
        del self.lines[num_lines:]
        if num_lines > 0:
            del self.lines[-1][num_pieces:]
        self.current_line_length = current_line_length
        assert self.get_state() == state

    def write(self, text: str) -> None:
        if not self.lines:
            self.start_new_line()
        self.lines[-1].append(text)
        self.line_has_content = True
        self.current_line_length += len(text)
        if self.can_split and self.current_line_length > self.line_length:
            raise LineTooLong

    def add_space(self) -> None:
        if self.lines and self.lines[-1] and not self.lines[-1][-1].endswith(" "):
            self.write(" ")

    def start_new_line(self) -> None:
        if self.lines and not self.line_has_content:
            return
        if self.lines and any(not text.isspace() for text in self.lines[-1]):
            self.lines[-1].append("\n")
        self.current_line_length = self.indent
        self.line_has_content = False
        line = []
        self.lines.append(line)
        if self.indent:
            line.append(" " * self.indent)

    def force_indentation(self) -> None:
        if self.line_has_content:
            self.start_new_line()
        else:
            needed = self.indent - self.current_line_length
            self.lines[-1].append(" " * needed)

    def clear_trailing_space(self) -> None:
        if self.lines[-1] and self.lines[-1][-1].endswith(" "):
            self.lines[-1][-1] = self.lines[-1][-1][:-1]
            self.current_line_length -= 1

    def add_comments(self, comments: Sequence[Token]) -> None:
        if comments:
            self.add_space()
        for comment in comments:
            self.write(comment.text.rstrip("\n"))
            self.start_new_line()

    def add_comments_from_leaf(self, node: p.Leaf) -> None:
        if not self.should_skip_comments:
            self.add_comments(node.token.comments)

    def visit_trailing_comma(self, node: Optional[p.Punctuation]) -> None:
        if node is not None:
            self.visit(node)
            self.add_space()

    @contextmanager
    def skip_comments(self) -> Generator[None, None, None]:
        old_value = self.should_skip_comments
        try:
            self.should_skip_comments = True
            yield
        finally:
            self.should_skip_comments = old_value

    def visit(self, node: p.Node) -> None:
        self.node_stack.append(node)
        try:
            if isinstance(node, p.Statement):
                for comment in node.leading_comments:
                    self.write(comment.text.rstrip("\n"))
                    self.start_new_line()
            super().visit(node)
            if isinstance(node, p.Leaf):
                self.add_comments_from_leaf(node)
        finally:
            self.node_stack.pop()

    def maybe_visit(
        self,
        node: Optional[p.Node],
        *,
        else_write: Optional[str] = None,
        add_space: bool = False,
    ) -> None:
        if node is None:
            if else_write is not None:
                self.write(else_write)
                if add_space:
                    self.add_space()
            return
        self.visit(node)
        if add_space:
            self.add_space()

    def write_punctuation(self, node: Optional[p.Punctuation]) -> None:
        if node is None:
            return
        if node.text not in ("(", ")", ",", "*"):
            self.add_space()
        self.visit_Punctuation(node)

    def parent_isinstance(self, node_cls: Type[p.Node]) -> bool:
        if len(self.node_stack) < 2:
            return False
        return isinstance(self.node_stack[-2], node_cls)

    def visit_KeywordSequence(self, node: p.KeywordSequence) -> None:
        # Move all the comments to the end
        with self.skip_comments():
            for i, kw in enumerate(node.keywords):
                if i > 0:
                    self.add_space()
                self.visit(kw)
        for kw in node.keywords:
            self.add_comments(kw.token.comments)

    def visit_FromClause(self, node: p.FromClause) -> None:
        if not self.parent_isinstance(p.Delete):
            self.start_new_line()
        if node.kw is None:
            self.write("FROM")
        else:
            self.visit(node.kw)
        self.write_comma_list(node.table)

    def write_comma_list(
        self, nodes: Sequence[p.WithTrailingComma[p.Node]], with_space: bool = True
    ) -> None:
        with self.override_can_split() as state:
            try:
                if with_space and nodes:
                    self.add_space()
                for node in nodes:
                    self.visit(node)
            except LineTooLong:
                pass
            else:
                return
        # Split any enclosing list first
        if self.can_split:
            raise LineTooLong
        self.restore_state(state)
        with self.add_indent():
            for node in nodes:
                self.start_new_line()
                self.visit(node)
                self.clear_trailing_space()
        self.start_new_line()

    def visit_GroupByClause(self, node: p.GroupByClause) -> None:
        self.start_new_line()
        self.visit(node.kwseq)
        self.write_comma_list(node.expr)

    def visit_OrderByClause(self, node: p.OrderByClause) -> None:
        self.start_new_line()
        self.visit(node.kwseq)
        self.write_comma_list(node.expr)

    def visit_SetClause(self, node: p.SetClause) -> None:
        self.start_new_line()
        self.visit(node.kw)
        self.write_comma_list(node.assignments)

    def visit_IntoClause(self, node: p.IntoClause) -> None:
        if node.kw is not None:
            self.visit(node.kw)
        else:
            self.write("INTO")
        self.add_space()
        self.visit(node.table)
        self.maybe_visit(node.col_names)

    def visit_ColNameList(self, node: p.ColNameList) -> None:
        self.visit(node.open_paren)
        self.write_comma_list(node.col_names, with_space=False)
        self.visit(node.close_paren)

    def visit_Subselect(
        self, node: p.Subselect, *, always_parenthesize: bool = False
    ) -> None:
        if node.left_paren is None:
            if always_parenthesize:
                self.write("(")
                with self.add_indent():
                    self.visit(node.select)
                self.write(")")
            else:
                self.visit(node.select)
        else:
            self.visit(node.left_paren)
            with self.add_indent():
                self.visit(node.select)
            assert node.right_paren is not None, "both parens must be set"
            self.visit(node.right_paren)

    def visit_ValuesClause(self, node: p.ValuesClause) -> None:
        self.start_new_line()
        if node.kw.text == "VALUES":
            self.visit(node.kw)
        else:
            self.write("VALUES")
            self.add_comments_from_leaf(node.kw)
        self.write_comma_list(node.value_lists)

    def visit_DefaultValues(self, node: p.DefaultValues) -> None:
        self.start_new_line()
        self.visit(node.kwseq)

    def visit_ValueList(self, node: p.ValueList) -> None:
        self.visit(node.open_paren)
        self.write_comma_list(node.values, with_space=False)
        self.visit(node.close_paren)

    def visit_Assignment(self, node: p.Assignment) -> None:
        self.visit(node.col_name)
        self.add_space()
        self.visit(node.eq_punc)
        self.add_space()
        self.visit(node.value)

    def visit_Default(self, node: p.Default) -> None:
        self.visit(node.kw)

    def visit_All(self, node: p.All) -> None:
        self.visit(node.kw)

    def visit_SelectLimitClause(self, node: p.SelectLimitClause) -> None:
        self.start_new_line()
        self.visit(node.kw)
        self.add_space()
        self.visit(node.row_count)
        if node.offset is not None:
            self.add_space()
            self.write("OFFSET")
            if node.offset_leaf is not None:
                self.add_comments_from_leaf(node.offset_leaf)
            self.add_space()
            self.visit(node.offset)

    def visit_CommonTableExpression(self, node: p.CommonTableExpression) -> None:
        self.visit(node.table_name)
        self.add_space()
        if node.col_names is not None:
            self.visit(node.col_names)
            self.add_space()
        self.visit(node.as_kw)
        self.add_space()
        self.visit(node.subquery)

    def visit_WithClause(self, node: p.WithClause) -> None:
        self.start_new_line()
        self.visit(node.kw)
        if node.recursive_kw is not None:
            self.add_space()
            self.visit(node.recursive_kw)
        self.write_comma_list(node.ctes)

    def visit_UsingClause(self, node: p.UsingClause) -> None:
        self.start_new_line()
        self.visit(node.kw)
        self.write_comma_list(node.tables)

    def visit_Select(self, node: p.Select) -> None:
        self.maybe_visit(node.with_clause)
        self.start_new_line()
        self.visit(node.select_kw)
        for kw in node.modifiers:
            self.add_space()
            self.visit(kw)
        self.write_comma_list(node.select_exprs)
        self.maybe_visit(node.into_before_from)
        self.maybe_visit(node.from_clause)
        self.maybe_visit(node.where)
        self.maybe_visit(node.group_by)
        self.maybe_visit(node.having)
        self.maybe_visit(node.order_by)
        self.maybe_visit(node.limit)
        self.maybe_visit(node.into_before_lock_mode)
        self.maybe_visit(node.lock_mode)
        self.maybe_visit(node.into)

    def visit_UnionStatement(self, node: p.UnionStatement) -> None:
        always_parens = bool(node.order_by or node.limit)
        self.visit_Subselect(node.first, always_parenthesize=always_parens)
        for entry in node.others:
            self.start_new_line()
            self.visit(entry.union_kw)
            if entry.all_kw:
                self.add_space()
                self.visit(entry.all_kw)
            self.start_new_line()
            self.visit_Subselect(entry.select, always_parenthesize=always_parens)
        self.maybe_visit(node.order_by)
        self.maybe_visit(node.limit)

    def visit_Delete(self, node: p.Delete) -> None:
        self.maybe_visit(node.with_clause)
        self.start_new_line()
        self.visit(node.delete_kw)
        self.add_space()
        self.visit(node.from_clause)
        self.maybe_visit(node.using_clause)
        self.maybe_visit(node.where)
        self.maybe_visit(node.order_by)
        self.maybe_visit(node.limit)

    def visit_Update(self, node: p.Update) -> None:
        self.maybe_visit(node.with_clause)
        self.start_new_line()
        self.visit(node.update_kw)
        self.add_space()
        self.visit(node.table)
        self.visit(node.set_clause)
        self.maybe_visit(node.where)
        self.maybe_visit(node.order_by)
        self.maybe_visit(node.limit)

    def _visit_insert_values(self, node: p.InsertValues) -> None:
        if isinstance(node, p.Subselect) and node.left_paren is not None:
            self.add_space()
        self.visit(node)

    def visit_Insert(self, node: p.Insert) -> None:
        self.start_new_line()
        self.visit(node.insert_kw)
        self.add_space()
        if node.ignore_kw is not None:
            self.visit(node.ignore_kw)
            self.add_space()
        self.visit(node.into)
        self._visit_insert_values(node.values)
        self.maybe_visit(node.odku)

    def visit_Replace(self, node: p.Replace) -> None:
        self.start_new_line()
        self.visit(node.replace_kw)
        self.add_space()
        self.visit(node.into)
        self._visit_insert_values(node.values)

    def visit_Keyword(self, node: p.Keyword) -> None:
        self.write(node.text.upper())

    def visit_Punctuation(self, node: p.Punctuation) -> None:
        self.write(node.text)

    def visit_KeywordIdentifier(self, node: p.KeywordIdentifier) -> None:
        self.visit(node.keyword)

    def visit_Identifier(self, node: p.Identifier) -> None:
        if (node.text.upper() in self.dialect.get_keywords()) or any(
            not c.isalnum() and c != "_" for c in node.text
        ):
            delimiter = self.dialect.get_identifier_delimiter()
            self.write(f"{delimiter}{node.text}{delimiter}")
        else:
            self.write(node.text)

    def visit_DottedTable(self, node: p.DottedTable) -> None:
        self.visit(node.left)
        self.visit(node.dot)
        self.visit(node.right)

    def visit_Dotted(self, node: p.Dotted) -> None:
        self.visit(node.left)
        self.visit(node.dot)
        self.visit(node.right)

    def visit_Placeholder(self, node: p.Placeholder) -> None:
        self.write(node.text)

    def visit_PlaceholderClause(self, node: p.PlaceholderClause) -> None:
        self.start_new_line()
        self.visit(node.placeholder)

    def visit_NumericLiteral(self, node: p.NumericLiteral) -> None:
        self.write(node.value.lower())

    def visit_StringLiteral(self, node: p.StringLiteral) -> None:
        inner = node.value.replace("'", "''")
        # ' is more portable
        self.write(f"'{inner}'")

    def visit_NullExpression(self, node: p.NullExpression) -> None:
        self.visit(node.null_kw)

    def visit_Star(self, node: p.Star) -> None:
        self.write("*")

    def visit_WithTrailingComma(self, node: p.WithTrailingComma) -> None:
        self.visit(node.node)
        self.visit_trailing_comma(node.trailing_comma)

    def visit_FunctionCall(self, node: p.FunctionCall) -> None:
        self.visit(node.callee)
        self.visit(node.left_paren)
        self.write_comma_list(node.args, with_space=False)
        self.visit(node.right_paren)

    def visit_Parenthesized(self, node: p.Parenthesized) -> None:
        self.visit(node.left_punc)
        self.visit(node.inner)
        self.visit(node.right_punc)

    def visit_ExprList(self, node: p.ExprList) -> None:
        self.visit(node.left_paren)
        self.write_comma_list(node.exprs, with_space=False)
        self.visit(node.right_paren)

    def visit_WhenThen(self, node: p.WhenThen) -> None:
        self.visit(node.when_kw)
        self.add_space()
        self.visit(node.condition)
        self.add_space()
        self.visit(node.then_kw)
        self.add_space()
        self.visit(node.result)

    def visit_ElseClause(self, node: p.ElseClause) -> None:
        self.visit(node.else_kw)
        self.add_space()
        self.visit(node.expr)

    def visit_CaseExpression(self, node: p.CaseExpression) -> None:
        self.visit(node.case_kw)
        self.add_space()
        self.maybe_visit(node.value, add_space=True)
        for when_then in node.when_thens:
            self.add_space()
            self.visit(when_then)
        self.add_space()
        self.maybe_visit(node.else_clause, add_space=True)
        self.visit(node.end_kw)

    def visit_Distinct(self, node: p.Distinct) -> None:
        self.visit(node.distinct_kw)
        self.add_space()
        self.visit(node.expr)

    def visit_GroupConcat(self, node: p.GroupConcat) -> None:
        self.visit(node.group_concat_kw)
        self.write_punctuation(node.left_paren)
        self.maybe_visit(node.distinct_kw, add_space=True)
        self.write_comma_list(node.exprs, with_space=False)
        if node.order_by is not None:
            self.add_space()
            if isinstance(node.order_by, p.OrderByClause):
                self.visit(node.order_by.kwseq)
                self.write_comma_list(node.order_by.expr)
            else:
                self.visit_Placeholder(node.order_by.placeholder)
        self.maybe_visit(node.separator)
        self.write_punctuation(node.right_paren)

    def visit_BinOp(self, node: p.BinOp) -> None:
        precedence = node.get_precedence()
        if precedence >= p.MIN_BOOLEAN_PRECEDENCE:
            self.clear_trailing_space()
            with self.add_indent():
                self.visit_BinOp_multiline(node)
            if self.parent_isinstance(p.Parenthesized):
                self.start_new_line()
        else:
            self.visit(node.left)
            self.add_space()
            self.visit(node.op)
            self.add_space()
            self.visit(node.right)

    def visit_UnaryOp(self, node: p.UnaryOp) -> None:
        self.visit(node.op)
        if not isinstance(
            node.expr,
            (p.Leaf, p.Parenthesized, p.Subselect, p.NullExpression, p.FunctionCall),
        ):
            self.write("(")
            self.visit(node.expr)
            self.write(")")
        else:
            self.visit(node.expr)

    def visit_BinOp_multiline(self, node: p.BinOp) -> None:
        precedence = node.get_precedence()
        self.force_indentation()
        self._maybe_multiline(node.left, precedence)
        self.start_new_line()
        self.visit(node.op)
        self.add_space()
        self._maybe_multiline(node.right, precedence)

    def _maybe_multiline(self, node: p.Node, precedence: int) -> None:
        if isinstance(node, p.BinOp) and node.get_precedence() == precedence:
            self.visit_BinOp_multiline(node)
        else:
            self.visit(node)

    def visit_IndexHint(self, node: p.IndexHint) -> None:
        self.start_new_line()
        self.visit(node.intro_kw)
        self.add_space()
        self.visit(node.kind_kw)
        if node.for_what is not None:
            self.add_space()
            if node.for_kw is not None:
                self.visit(node.for_kw)
            else:
                self.write("FOR")
            self.add_space()
            self.visit(node.for_what)
        self.visit(node.left_paren)
        self.write_comma_list(node.index_list, with_space=False)
        self.visit(node.right_paren)

    def visit_JoinOn(self, node: p.JoinOn) -> None:
        self.start_new_line()
        self.visit(node.kw)
        self.add_space()
        self.visit(node.search_condition)

    def visit_SimpleJoinedTable(self, node: p.SimpleJoinedTable):
        self.visit_join(node)

    def visit_LeftRightJoinedTable(self, node: p.LeftRightJoinedTable):
        self.visit_join(node)

    def visit_NaturalJoinedTable(self, node: p.NaturalJoinedTable):
        self.visit_join(node)

    def visit_join(self, node: p.JoinedTable, *, skip_indent: bool = False) -> None:
        if isinstance(node, (p.SimpleJoinedTable, p.LeftRightJoinedTable)):
            join_spec = node.join_specification
        else:
            join_spec = None
        if isinstance(node, p.SimpleJoinedTable):
            kws = [node.inner_cross, node.join_kw]
        elif isinstance(node, p.LeftRightJoinedTable):
            kws = [node.left_right, node.outer_kw, node.join_kw]
        else:
            kws = [node.natural_kw, node.left_right, node.inner_outer, node.join_kw]
        if isinstance(
            node.left,
            (p.SimpleJoinedTable, p.LeftRightJoinedTable, p.NaturalJoinedTable),
        ):
            self.visit(node.left)
        else:
            self.clear_trailing_space()
            with self.add_indent():
                self.start_new_line()
                self.visit(node.left)
        self.start_new_line()
        kws = [kw for kw in kws if kw is not None]
        self.visit_KeywordSequence(p.KeywordSequence(kws))
        with self.add_indent():
            self.start_new_line()
            self.visit(node.right)
        self.maybe_visit(join_spec)

    def visit_SimpleTableFactor(self, node: p.SimpleTableFactor) -> None:
        self.visit(node.table_name)
        if node.alias is not None:
            self.add_space()
            self.maybe_visit(node.as_kw, else_write="AS", add_space=True)
            self.visit(node.alias)
        for index_hint in node.index_hint_list:
            self.start_new_line()
            self.visit(index_hint.node)
            if index_hint.trailing_comma is not None:
                self.visit(index_hint.trailing_comma)

    def visit_SimpleTableName(self, node: p.SimpleTableName) -> None:
        self.visit(node.identifier)

    def visit_SubQueryFactor(self, node: p.SubqueryFactor) -> None:
        self.maybe_visit(node.lateral_kw, add_space=True)
        self.visit(node.table_subquery)
        self.maybe_visit(node.as_kw, else_write="AS", add_space=True)
        self.visit(node.alias)
        if node.col_list:
            self.maybe_visit(node.left_paren, else_write="(")
            self.write_comma_list(node.col_list)
            self.maybe_visit(node.right_paren, else_write=")")

    def visit_TableReferenceList(self, node: p.TableReferenceList) -> None:
        self.visit(node.left_paren)
        self.write_comma_list(node.references, with_space=False)
        self.visit(node.right_paren)

    def visit_StartTransaction(self, node: p.StartTransaction) -> None:
        self.visit(node.start_kw)
        self.add_space()
        self.visit(node.transaction_kw)
        if node.characteristics:
            self.add_space()
            self.write_comma_list(node.characteristics)

    def visit_Flush(self, node: p.Flush) -> None:
        self.visit(node.flush_kw)
        self.add_space()
        self.maybe_visit(node.modifier, add_space=True)
        if isinstance(node.option, p.TablesOption):
            self.visit(node.option)
        else:
            self.write_comma_list(node.option)

    def visit_LikeTable(self, node: p.LikeTable) -> None:
        self.visit(node.like_kw)
        self.add_space()
        self.visit(node.table)

    def generic_visit(self, node: p.Node) -> None:
        """For unhandled nodes, we try to generate the formatter."""
        typ = type(node)
        lines = []
        is_statement = isinstance(node, p.Statement)
        is_clause = isinstance(node, p.Clause)
        if is_statement or is_clause:
            lines.append("self.start_new_line()")
        last_was_paren = False
        for i, field_obj in enumerate(fields(typ)):
            lines += _get_lines_for_field(
                node,
                i,
                field_obj,
                is_statement=is_statement,
                is_clause=is_clause,
                last_was_paren=last_was_paren,
            )
            last_was_paren = field_obj.name == "left_paren"

        body = "".join(f"    {line}\n" for line in lines)
        func_name = f"visit_{typ.__name__}"
        code = f"def {func_name}(self, node: p.{typ.__name__}) -> None:\n{body}"
        ns: Dict[str, Any] = {"p": p}
        exec(code, ns)
        func = ns[func_name]
        func(self, node)
        setattr(type(self), func_name, func)


def _get_lines_for_field(
    node: p.Node,
    i: int,
    field_obj: Field,
    *,
    is_statement: bool,
    is_clause: bool,
    last_was_paren: bool,
) -> Iterator[str]:
    if is_statement and field_obj.name == "leading_comments":
        return
    origin, args = _get_origin_args(field_obj.type)
    if (origin in (typing.Sequence, collections.abc.Sequence)) and len(args) == 1:
        sub_origin, _ = _get_origin_args(args[0])
        if sub_origin is p.WithTrailingComma:
            yield f"self.write_comma_list(node.{field_obj.name})"
            return
    if origin is Union:
        is_optional = NoneType in args
        types = {t for t in args if t is not NoneType}
    else:
        is_optional = False
        types = {field_obj.type}
    if (
        types
        <= {
            p.Keyword,
            p.KeywordSequence,
            p.StringLiteral,
            p.Identifier,
            p.Expression,
            p.DottedTable,
            p.SimpleTableName,
            p.FunctionCall,
            p.CharType,
            p.CharsetInfo,
            p.Placeholder,
            p.LikeTable,
            p.ParenthesizedLikeTable,
        }
        and not last_was_paren
    ):
        if is_optional:
            if i == 0 and (is_statement or is_clause):
                raise NotImplementedError(f"{type(node)}")
            yield f"if node.{field_obj.name} is not None:"
            yield "    self.add_space()"
            yield f"    self.visit(node.{field_obj.name})"
        else:
            if i > 0 or (not is_statement and not is_clause):
                yield "self.add_space()"
            yield f"self.visit(node.{field_obj.name})"
    elif types == {p.Punctuation}:
        yield f"self.write_punctuation(node.{field_obj.name})"
    elif all(isinstance(t, type) and issubclass(t, p.Node) for t in types):
        if is_optional:
            yield f"self.maybe_visit(node.{field_obj.name})"
        else:
            yield f"self.visit(node.{field_obj.name})"
    else:
        raise NotImplementedError(f"{type(node)}: {field_obj}")


def _get_origin_args(obj: Any) -> Tuple[object, Sequence[object]]:
    if sys.version_info >= (3, 8):
        return typing.get_origin(obj), typing.get_args(obj)
    elif sys.version_info >= (3, 7):
        if hasattr(obj, "__origin__"):
            return obj.__origin__, obj.__args__
        return None, ()
    else:
        if isinstance(obj, tuple) and len(obj) == 2:
            return obj
        if hasattr(obj, "_subs_tree"):
            origin, *args = obj._subs_tree()
            return origin, args
        return None, ()


def format_tree(
    tree: p.Node,
    *,
    dialect: Dialect = DEFAULT_DIALECT,
    line_length: int = DEFAULT_LINE_LENGTH,
    indent: int = 0,
) -> str:
    return Formatter(dialect, line_length=line_length, indent=indent).format(tree)


def format(
    sql: str,
    dialect: Dialect = DEFAULT_DIALECT,
    *,
    line_length: int = DEFAULT_LINE_LENGTH,
    indent: int = 0,
) -> str:
    return format_tree(
        sqltree(sql, dialect), dialect=dialect, line_length=line_length, indent=indent
    )


def transform_and_format(sql: str, transformer: Transformer) -> str:
    tree = sqltree(sql)
    new_tree = transformer.visit(tree)
    return format_tree(new_tree)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("sqltree")
    parser.add_argument("sql", help="SQL string to format")
    args = parser.parse_args()
    print(format(args.sql), end="")
