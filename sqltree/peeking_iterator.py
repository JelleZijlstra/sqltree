from dataclasses import dataclass, field
from typing import Generic, Optional, Sequence, TypeVar

T = TypeVar("T")


@dataclass
class PeekingIterator(Generic[T]):
    seq: Sequence[T]
    next_pos: int = 0
    length: int = field(init=False)

    def __post_init__(self) -> None:
        self.length = len(self.seq)

    def next(self) -> T:
        next_pos = self.next_pos
        if next_pos < self.length:
            self.next_pos = next_pos + 1
            return self.seq[next_pos]
        raise StopIteration

    def __iter__(self) -> "PeekingIterator[T]":
        return self

    def __next__(self) -> T:
        return self.next()

    def wind_back(self) -> None:
        self.next_pos -= 1

    def advance(self) -> None:
        self.next_pos += 1

    def has_next(self) -> bool:
        return self.next_pos < self.length

    def current(self) -> T:
        return self.seq[self.next_pos - 1]

    def peek_or_raise(self) -> T:
        return self.seq[self.next_pos]

    def peek(self) -> Optional[T]:
        if self.next_pos < self.length:
            return self.seq[self.next_pos]
        return None
