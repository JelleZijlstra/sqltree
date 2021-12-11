from dataclasses import dataclass, field


@dataclass
class Location:
    sql: str = field(repr=False)
    start_index: int
    end_index: int  # index of the last character included in the token
