from dataclasses import dataclass
from typing import Generic, Optional, Sequence, TypeVar

T = TypeVar("T")


@dataclass
class PeekingIterator(Generic[T]):
    seq: Sequence[T]
    next_pos: int = 0

    def next(self) -> T:
        if self.has_next():
            self.next_pos += 1
            return self.seq[self.next_pos - 1]
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
        return self.next_pos < len(self.seq)

    def current(self) -> T:
        return self.seq[self.next_pos - 1]

    def peek(self) -> Optional[T]:
        if self.next_pos < len(self.seq):
            return self.seq[self.next_pos]
        return None
