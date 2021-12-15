import argparse
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, Iterator, List, Optional, Sequence

from sqltree.dialect import DEFAULT_DIALECT, Dialect

from . import parser as p
from .sqltree import sqltree
from .tokenizer import Token
from .visitor import Transformer, Visitor

DEFAULT_LINE_LENGTH = 88  # like black


class LineTooLong(Exception):
    """Raised internally when a line is about to get too long."""


@dataclass
class Formatter(Visitor[None]):
    line_length: int = DEFAULT_LINE_LENGTH
    indent: int = 0
    lines: List[List[str]] = field(default_factory=list)
    should_skip_comments: bool = False
    current_line_length: int = 0
    can_split: bool = False
    node_stack: List[p.Node] = field(default_factory=list)

    def format(self, tree: p.Node) -> str:
        self.visit(tree)
        sql = "".join(piece for line in self.lines for piece in line)
        if self.indent > 0:
            return f"\n{sql}\n{' ' * self.indent}"
        else:
            return sql + "\n"

    @contextmanager
    def add_indent(self) -> Iterator[None]:
        self.indent += 4
        try:
            yield
        finally:
            self.indent -= 4

    @contextmanager
    def override_can_split(self) -> Iterator[None]:
        previous_val = self.can_split
        self.can_split = True
        try:
            yield
        finally:
            self.can_split = previous_val

    def write(self, text: str) -> None:
        if not self.lines:
            self.start_new_line()
        self.lines[-1].append(text)
        self.current_line_length += len(text)
        if self.can_split and self.current_line_length > self.line_length:
            raise LineTooLong

    def add_space(self) -> None:
        if self.lines and self.lines[-1] and not self.lines[-1][-1].endswith(" "):
            self.write(" ")

    def start_new_line(self) -> None:
        if self.lines and any(not text.isspace() for text in self.lines[-1]):
            self.lines[-1].append("\n")
        self.lines.append([])
        if self.indent:
            self.write(" " * self.indent)

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
        if not isinstance(self.node_stack[-2], p.Delete):
            self.start_new_line()
        if node.kw is None:
            self.write("FROM")
        else:
            self.visit(node.kw)
        self.add_space()
        self.visit(node.table)

    def visit_WhereClause(self, node: p.WhereClause) -> None:
        self.start_new_line()
        self.visit(node.kw)
        self.add_space()
        self.visit(node.conditions)

    def visit_HavingClause(self, node: p.HavingClause) -> None:
        self.start_new_line()
        self.visit(node.kw)
        self.add_space()
        self.visit(node.conditions)

    def write_comma_list(
        self, nodes: Sequence[p.WithTrailingComma[p.Node]], with_space: bool = True
    ) -> None:
        if with_space:
            self.add_space()
        for node in nodes:
            self.visit(node)

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

    def visit_Subselect(self, node: p.Subselect) -> None:
        if node.left_paren is None:
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

    def visit_OdkuClause(self, node: p.OdkuClause) -> None:
        self.start_new_line()
        self.visit(node.kwseq)
        self.write_comma_list(node.assignments)

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

    def visit_LimitClause(self, node: p.LimitClause) -> None:
        self.start_new_line()
        self.visit(node.kw)
        self.add_space()
        self.visit(node.row_count)

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
        self.write_comma_list(node.select_exprs)
        self.maybe_visit(node.from_clause)
        self.maybe_visit(node.where)
        self.maybe_visit(node.group_by)
        self.maybe_visit(node.having)
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

    def visit_Identifier(self, node: p.Identifier) -> None:
        self.write(node.text)

    def visit_Placeholder(self, node: p.Placeholder) -> None:
        self.write(node.text)

    def visit_IntegerLiteral(self, node: p.IntegerLiteral) -> None:
        self.write(str(node.value))

    def visit_StringLiteral(self, node: p.StringLiteral) -> None:
        self.write(f'"{node.value}"')

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

    def visit_BinOp(self, node: p.BinOp) -> None:
        self.visit(node.left)
        self.add_space()
        self.visit(node.op)
        self.add_space()
        self.visit(node.right)

    def visit_SelectExpr(self, node: p.SelectExpr) -> None:
        self.visit(node.expr)
        if node.as_kw is not None and node.alias is not None:
            self.add_space()
            self.visit(node.as_kw)
            self.add_space()
            self.visit(node.alias)

    def visit_OrderByExpr(self, node: p.OrderByExpr) -> None:
        self.visit(node.expr)
        if node.direction_kw is not None:
            self.add_space()
            self.visit(node.direction_kw)


def format_tree(
    tree: p.Node, *, line_length: int = DEFAULT_LINE_LENGTH, indent: int = 0
) -> str:
    return Formatter(line_length=line_length, indent=indent).format(tree)


def format(
    sql: str,
    dialect: Dialect = DEFAULT_DIALECT,
    *,
    line_length: int = DEFAULT_LINE_LENGTH,
    indent: int = 0,
) -> str:
    return format_tree(sqltree(sql, dialect), line_length=line_length, indent=indent)


def transform_and_format(sql: str, transformer: Transformer) -> str:
    tree = sqltree(sql)
    new_tree = transformer.visit(tree)
    return format_tree(new_tree)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("sqltree")
    parser.add_argument("sql", help="SQL string to format")
    args = parser.parse_args()
    print(format(args.sql), end="")
