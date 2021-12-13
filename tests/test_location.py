from sqltree.location import Location


def test_location() -> None:
    loc = Location("x", 0, 0)
    assert loc.display() == "0: x\n   ^\n"

    loc = Location("x\ny", 2, 2)
    assert loc.display() == "1: y\n   ^\n"

    loc = Location("a\nbcde", 3, 4)
    assert loc.display() == "1: bcde\n    ^^\n"

    loc = Location("a\nbcde\nf", 3, 4)
    assert loc.display() == "1: bcde\n    ^^\n"
