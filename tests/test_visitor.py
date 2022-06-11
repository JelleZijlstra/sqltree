from sqltree.formatter import transform_and_format
from sqltree.parser import BinOp, Keyword, Punctuation
from sqltree.sqltree import sqltree
from sqltree.visitor import Transformer, walk


class FlipConditions(Transformer):
    def visit_BinOp(self, node: BinOp) -> BinOp:
        if node.op.text == "=":
            op = Punctuation(node.op.token, "!=")
        else:
            op = node.op
        return BinOp(node.left, op, node.right)


def test_transform() -> None:
    assert (
        transform_and_format("SELECT * FROM x WHERE a = 3", FlipConditions())
        == "SELECT *\nFROM x\nWHERE a != 3\n"
    )


def test_walk() -> None:
    sql = "SELECT * FROM x WHERE a = 3"
    tree = sqltree(sql)
    nodes = list(walk(tree))
    assert any(isinstance(node, Punctuation) and node.text == "=" for node in nodes)
    assert any(isinstance(node, Keyword) and node.text == "WHERE" for node in nodes)
    assert {node.text for node in nodes if isinstance(node, Keyword)} == {
        "SELECT",
        "FROM",
        "WHERE",
    }
