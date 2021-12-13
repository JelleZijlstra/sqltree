import collections.abc
from dataclasses import fields
from typing import Generic, Optional, TypeVar

from .parser import Node

T = TypeVar("T")


class Visitor(Generic[T]):
    def visit(self, node: Node) -> T:
        method_name = f"visit_{type(node).__name__}"
        method = getattr(self, method_name, self.generic_visit)
        return method(node)  # type: ignore

    def maybe_visit(self, node: Optional[Node]) -> Optional[T]:
        if node is None:
            return None
        return self.visit(node)

    def generic_visit(self, node: Node) -> T:
        raise NotImplementedError(node)


class Transformer(Visitor[Node]):
    def generic_visit(self, node: Node) -> Node:
        cls = type(node)
        kwargs = {}
        for field in fields(node):
            key = field.name
            value = getattr(node, key)
            if isinstance(value, Node):
                kwargs[key] = self.visit(value)
            elif isinstance(value, collections.abc.Sequence) and not isinstance(
                value, str
            ):
                kwargs[key] = [self.visit(member) for member in value]
            else:
                kwargs[key] = value
        return cls(**kwargs)
