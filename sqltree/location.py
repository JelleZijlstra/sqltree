from dataclasses import dataclass, field


@dataclass
class Location:
    sql: str = field(repr=False)
    start_index: int
    end_index: int  # index of the last character included in the token

    def display(self) -> str:
        is_past_end = self.start_index >= len(self.sql)
        starting_lineno = self.sql.count("\n", 0, self.start_index)
        try:
            previous_newline = self.sql.rindex("\n", 0, self.start_index)
        except ValueError:
            previous_newline = -1
        try:
            following_newline = self.sql.index("\n", self.end_index)
        except ValueError:
            following_newline = len(self.sql)
        ending_lineno = self.sql.count("\n", 0, following_newline)
        lineno_length = len(str(ending_lineno))
        pieces = []
        pieces.append(f"{starting_lineno:{lineno_length}}: ")
        pieces.append(self.sql[previous_newline + 1 : self.start_index])
        if is_past_end:
            matching_pieces = [" "]
        else:
            matching_pieces = self.sql[self.start_index : self.end_index + 1].split(
                "\n"
            )
        leading_length = lineno_length + 2 + (self.start_index - previous_newline - 1)
        remaining_carets = None
        for i, piece in enumerate(matching_pieces):
            if remaining_carets is not None:
                pieces.append(remaining_carets)
            if i > 0:
                pieces.append("\n")
            pieces.append(piece)
            remaining_carets = "\n" + " " * leading_length + "^" * len(piece)
            leading_length = lineno_length + 2
        rest = self.sql[self.end_index + 1 : following_newline]
        pieces.append(rest)
        if remaining_carets:
            pieces.append(remaining_carets)
        pieces.append("\n")
        return "".join(pieces)
