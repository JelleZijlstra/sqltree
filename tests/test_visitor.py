from sqltree.formatter import transform_and_format
from sqltree.parser import BinOp, Punctuation
from sqltree.visitor import Transformer


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
