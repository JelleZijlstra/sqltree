import argparse
from dataclasses import dataclass, field
from typing import List

from . import parser as p
from .sqltree import sqltree
from .visitor import Visitor


@dataclass
class Formatter(Visitor[None]):
    pieces: List[str] = field(default_factory=list)

    def visit(self, node: p.Node) -> None:
        if isinstance(node, p.Statement):
            for comment in node.leading_comments:
                self.pieces.append(comment.text)
        super().visit(node)
        if isinstance(node, p.Leaf):
            if node.token.comments:
                self.pieces.append(" ")
            for comment in node.token.comments:
                self.pieces.append(comment.text)

    def visit_Select(self, node: p.Select) -> None:
        self.visit(node.select_kw)
        self.pieces.append(" ")
        for i, expr in enumerate(node.select_exprs):
            if i > 0:
                self.pieces.append(" ")
            self.visit(expr)
        self.pieces.append("\n")
        if node.from_kw is not None and node.table is not None:
            self.visit(node.from_kw)
            self.pieces.append(" ")
            self.visit(node.table)
            self.pieces.append("\n")
        if node.where_kw is not None and node.conditions is not None:
            self.visit(node.where_kw)
            self.pieces.append(" ")
            self.visit(node.conditions)
            self.pieces.append("\n")

    def visit_Keyword(self, node: p.Keyword) -> None:
        self.pieces.append(node.text)

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
        self.pieces.append(" ")
        self.visit(node.op)
        self.pieces.append(" ")
        self.visit(node.right)

    def visit_SelectExpr(self, node: p.SelectExpr) -> None:
        self.visit(node.expr)
        if node.as_kw is not None and node.alias is not None:
            self.visit(node.as_kw)
            self.pieces.append(" ")
            self.visit(node.alias)
        if node.trailing_comma is not None:
            self.visit(node.trailing_comma)


def format(sql: str) -> str:
    tree = sqltree(sql)
    fmt = Formatter()
    fmt.visit(tree)
    return "".join(fmt.pieces)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("sqltree")
    parser.add_argument("sql", help="SQL string to format")
    args = parser.parse_args()
    print(format(args.sql), end="")
