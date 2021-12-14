from sqltree.dialect import Dialect, Vendor
from sqltree.formatter import format


def test_select() -> None:
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
    assert format("select x from y order by z") == "SELECT x\nFROM y\nORDER BY z\n"
    assert format("select x from y limit 1") == "SELECT x\nFROM y\nLIMIT 1\n"
    assert (
        format("select x from y limit 1, -- hi\n2")
        == "SELECT x\nFROM y\nLIMIT 2 OFFSET -- hi\n1\n"
    )
    assert (
        format("select x from y limit 1 offset 2")
        == "SELECT x\nFROM y\nLIMIT 1 OFFSET 2\n"
    )


def test_update() -> None:
    assert (
        format("update x set y = default, z =3 where x=4 order   by z limit 1")
        == "UPDATE x\nSET y = DEFAULT, z = 3\nWHERE x = 4\nORDER BY z\nLIMIT 1\n"
    )


def test_delete() -> None:
    assert (
        format("delete from x where y = 3 order by z desc limit 1")
        == "DELETE FROM x\nWHERE y = 3\nORDER BY z DESC\nLIMIT 1\n"
    )


def test_insert() -> None:
    assert (
        format("insert x (a, b, c) value(1, 2,3), (4,5,6) on duplicate key update a=4")
        == "INSERT INTO x(a, b, c)\nVALUES (1, 2, 3), (4, 5, 6)\nON DUPLICATE KEY"
        " UPDATE a = 4\n"
    )
    assert (
        format("insert ignore into x(a) values(1)")
        == "INSERT IGNORE INTO x(a)\nVALUES (1)\n"
    )
    assert (
        format("insert into x(a) values(1)", Dialect(Vendor.redshift))
        == "INSERT INTO x(a)\nVALUES (1)\n"
    )
    assert (
        format("insert into x(a) default   values", Dialect(Vendor.redshift))
        == "INSERT INTO x(a)\nDEFAULT VALUES\n"
    )
    assert (
        format("insert into x(a) (select x from y)", Dialect(Vendor.redshift))
        == "INSERT INTO x(a)\n(SELECT x\nFROM y\n)\n"
    )
    assert (
        format("insert into x(a) select x from y")
        == "INSERT INTO x(a)\nSELECT x\nFROM y\n"
    )


def test_replace() -> None:
    assert format("replace into x(a) values(1)") == "REPLACE INTO x(a)\nVALUES (1)\n"
