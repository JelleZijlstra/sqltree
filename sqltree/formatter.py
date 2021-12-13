import argparse
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, List, Optional, Sequence

from . import parser as p
from .sqltree import sqltree
from .tokenizer import Token
from .visitor import Transformer, Visitor


@dataclass
class Formatter(Visitor[None]):
    pieces: List[str] = field(default_factory=list)
    should_skip_comments: bool = False

    def add_space(self) -> None:
        if self.pieces and not self.pieces[-1].endswith("\n"):
            self.pieces.append(" ")

    def add_comments(self, comments: Sequence[Token]) -> None:
        if comments:
            self.add_space()
        for comment in comments:
            self.pieces.append(comment.text)

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
        if isinstance(node, p.Statement):
            for comment in node.leading_comments:
                self.pieces.append(comment.text)
        super().visit(node)
        if isinstance(node, p.Leaf):
            self.add_comments_from_leaf(node)

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
        self.visit(node.kw)
        self.add_space()
        self.visit(node.table)
        self.pieces.append("\n")

    def visit_WhereClause(self, node: p.WhereClause) -> None:
        self.visit(node.kw)
        self.add_space()
        self.visit(node.conditions)
        self.pieces.append("\n")

    def visit_HavingClause(self, node: p.HavingClause) -> None:
        self.visit(node.kw)
        self.add_space()
        self.visit(node.conditions)
        self.pieces.append("\n")

    def visit_GroupByClause(self, node: p.GroupByClause) -> None:
        self.visit(node.kwseq)
        self.add_space()
        for expr in node.expr:
            self.visit(expr)
        self.pieces.append("\n")

    def visit_OrderByClause(self, node: p.OrderByClause) -> None:
        self.visit(node.kwseq)
        self.add_space()
        for expr in node.expr:
            self.visit(expr)
        self.pieces.append("\n")

    def visit_SetClause(self, node: p.SetClause) -> None:
        self.visit(node.kw)
        self.add_space()
        for assignment in node.assignments:
            self.visit(assignment)
        self.pieces.append("\n")

    def visit_IntoClause(self, node: p.IntoClause) -> None:
        if node.kw is not None:
            self.visit(node.kw)
        else:
            self.pieces.append("INTO")
        self.add_space()
        self.visit(node.table)
        if node.col_names:
            if node.open_paren is not None:
                self.visit(node.open_paren)
            else:
                self.pieces.append("(")
            for col_name in node.col_names:
                self.visit(col_name)
            if node.close_paren is not None:
                self.visit(node.close_paren)
            else:
                self.pieces.append(")")
        self.pieces.append("\n")

    def visit_ColName(self, node: p.ColName) -> None:
        self.visit(node.col_name)
        self.visit_trailing_comma(node.trailing_comma)

    def visit_ValuesClause(self, node: p.ValuesClause) -> None:
        if node.kw.text == "VALUES":
            self.visit(node.kw)
        else:
            self.pieces.append("VALUES")
            self.add_comments_from_leaf(node.kw)
        self.add_space()
        for value_list in node.value_lists:
            self.visit(value_list)
        self.pieces.append("\n")

    def visit_ValueList(self, node: p.ValueList) -> None:
        self.visit(node.open_paren)
        for value in node.values:
            self.visit(value)
        self.visit(node.close_paren)
        self.visit_trailing_comma(node.trailing_comma)

    def visit_ValueWithComma(self, node: p.ValueWithComma) -> None:
        self.visit(node.value)
        self.visit_trailing_comma(node.trailing_comma)

    def visit_OdkuClause(self, node: p.OdkuClause) -> None:
        self.visit(node.kwseq)
        self.add_space()
        for assignment in node.assignments:
            self.visit(assignment)
        self.pieces.append("\n")

    def visit_Assignment(self, node: p.Assignment) -> None:
        self.visit(node.col_name)
        self.add_space()
        self.visit(node.eq_punc)
        self.add_space()
        self.visit(node.value)
        self.visit_trailing_comma(node.trailing_comma)

    def visit_Default(self, node: p.Default) -> None:
        self.visit(node.kw)

    def visit_LimitClause(self, node: p.LimitClause) -> None:
        self.visit(node.kw)
        self.add_space()
        self.visit(node.row_count)
        self.pieces.append("\n")

    def visit_Select(self, node: p.Select) -> None:
        self.visit(node.select_kw)
        self.add_space()
        for expr in node.select_exprs:
            self.visit(expr)
        self.pieces.append("\n")

        self.maybe_visit(node.from_clause)
        self.maybe_visit(node.where)
        self.maybe_visit(node.group_by)
        self.maybe_visit(node.having)
        self.maybe_visit(node.order_by)

    def visit_Delete(self, node: p.Delete) -> None:
        self.visit(node.delete_kw)
        self.add_space()
        self.visit(node.from_clause)
        self.maybe_visit(node.where)
        self.maybe_visit(node.order_by)
        self.maybe_visit(node.limit)

    def visit_Update(self, node: p.Update) -> None:
        self.visit(node.update_kw)
        self.add_space()
        self.visit(node.table)
        self.pieces.append("\n")
        self.visit(node.set_clause)
        self.maybe_visit(node.where)
        self.maybe_visit(node.order_by)
        self.maybe_visit(node.limit)

    def visit_Insert(self, node: p.Insert) -> None:
        self.visit(node.insert_kw)
        self.add_space()
        self.visit(node.into)
        self.visit(node.values)
        self.maybe_visit(node.odku)

    def visit_Keyword(self, node: p.Keyword) -> None:
        self.pieces.append(node.text.upper())

    def visit_Punctuation(self, node: p.Punctuation) -> None:
        self.pieces.append(node.text)

    def visit_Identifier(self, node: p.Identifier) -> None:
        self.pieces.append(node.text)

    def visit_Placeholder(self, node: p.Placeholder) -> None:
        self.pieces.append(node.text)

    def visit_IntegerLiteral(self, node: p.IntegerLiteral) -> None:
        self.pieces.append(str(node.value))

    def visit_StringLiteral(self, node: p.StringLiteral) -> None:
        self.pieces.append(f'"{node.value}"')

    def visit_Star(self, node: p.Star) -> None:
        self.pieces.append("*")

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
        self.visit_trailing_comma(node.trailing_comma)

    def visit_OrderByExpr(self, node: p.OrderByExpr) -> None:
        self.visit(node.expr)
        if node.direction_kw is not None:
            self.add_space()
            self.visit(node.direction_kw)
        self.visit_trailing_comma(node.trailing_comma)


def format_tree(tree: p.Node) -> str:
    fmt = Formatter()
    fmt.visit(tree)
    return "".join(fmt.pieces)


def format(sql: str) -> str:
    return format_tree(sqltree(sql))


def transform_and_format(sql: str, transformer: Transformer) -> str:
    tree = sqltree(sql)
    new_tree = transformer.visit(tree)
    return format_tree(new_tree)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("sqltree")
    parser.add_argument("sql", help="SQL string to format")
    args = parser.parse_args()
    print(format(args.sql), end="")
