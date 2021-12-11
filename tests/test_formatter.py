from sqltree.formatter import format


def test() -> None:
    assert format("SELECT * from x") == "SELECT *\nFROM x\n"
    assert (
        format("# comment\nSELECT * from x where -- comment\n x=3")
        == "# comment\nSELECT *\nFROM x\nWHERE -- comment\nx = 3\n"
    )
    assert format("SELECT x not -- hello\nLIKE y") == "SELECT x NOT LIKE -- hello\ny\n"
    assert (
        format(
            "select x as   z from y group  -- hello\n  by x asc,y desc having x={x}"
            " order  by x desc"
        )
        == "SELECT x AS z\nFROM y\nGROUP BY -- hello\nx ASC, y DESC\nHAVING x ="
        " {x}\nORDER"
        " BY x DESC\n"
    )
