from typing import Generic, TypeVar

from .parser import Node

T = TypeVar("T")


class Visitor(Generic[T]):
    def visit(self, node: Node) -> T:
        method_name = f"visit_{type(node).__name__}"
        method = getattr(self, method_name, self.generic_visit)
        return method(node)  # type: ignore

    def generic_visit(self, node: Node) -> T:
        raise NotImplementedError(node)
