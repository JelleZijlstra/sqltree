import pytest

from sqltree.dialect import Dialect, Vendor
from sqltree.formatter import format
from sqltree.parser import ParseError


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
    offset_comma = "select x from y limit 1, -- hi\n2"
    assert format(offset_comma) == "SELECT x\nFROM y\nLIMIT 2 OFFSET -- hi\n1\n"
    with pytest.raises(ParseError):
        format(offset_comma, Dialect(Vendor.redshift))
    assert (
        format("select x from y limit 1 offset 2")
        == "SELECT x\nFROM y\nLIMIT 1 OFFSET 2\n"
    )
    assert (
        format("with x as (select x from y) select y from x", Dialect(Vendor.redshift))
        == "WITH x AS (\n    SELECT x\n    FROM y)\nSELECT y\nFROM x\n"
    )
    select_limit_all = "select y from x limit all"
    assert (
        format(select_limit_all, Dialect(Vendor.redshift))
        == "SELECT y\nFROM x\nLIMIT ALL\n"
    )
    with pytest.raises(ParseError):
        format(select_limit_all)

    assert (
        format(
            "select max(x) from y where x = some(function, many, args) and y ="
            " no_args()",
            indent=8,
        )
        == """
        SELECT max(x)
        FROM y
        WHERE
            x = some(function, many, args)
            AND y = no_args()
        """
    )
    assert (
        format(
            "select aaaaaa, bbbbbbbb, ccccc, dddddd from x", line_length=20, indent=8
        )
        == """
        SELECT
            aaaaaa,
            bbbbbbbb,
            ccccc,
            dddddd
        FROM x
        """
    )
    assert (
        format("select max(x) from y where x = 1 and y = 2 and z = 3 limit 4", indent=8)
        == """
        SELECT max(x)
        FROM y
        WHERE
            x = 1
            AND y = 2
            AND z = 3
        LIMIT 4
        """
    )


def test_update() -> None:
    assert (
        format("update x set y = default, z =3 where x=4 order   by z limit 1")
        == "UPDATE x\nSET y = DEFAULT, z = 3\nWHERE x = 4\nORDER BY z\nLIMIT 1\n"
    )
    assert (
        format(
            "with x as (select x from y) update y set x = 3", Dialect(Vendor.redshift)
        )
        == "WITH x AS (\n    SELECT x\n    FROM y)\nUPDATE y\nSET x = 3\n"
    )
    update_limit = "update y set x = 3 limit 1"
    with pytest.raises(ParseError):
        format(update_limit, Dialect(Vendor.redshift))
    assert format(update_limit) == "UPDATE y\nSET x = 3\nLIMIT 1\n"


def test_delete() -> None:
    assert (
        format("delete from x where y = 3 order by z desc limit 1")
        == "DELETE FROM x\nWHERE y = 3\nORDER BY z DESC\nLIMIT 1\n"
    )
    assert (
        format("with x as (select x from y) delete from y", Dialect(Vendor.redshift))
        == "WITH x AS (\n    SELECT x\n    FROM y)\nDELETE FROM y\n"
    )
    assert (
        format(
            "with x as (select x from y) delete from y using z, a where x = 4",
            Dialect(Vendor.redshift),
            indent=8,
        )
        == """
        WITH x AS (
            SELECT x
            FROM y)
        DELETE FROM y
        USING z, a
        WHERE x = 4
        """
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
        == "INSERT INTO x(a) (\n    SELECT x\n    FROM y)\n"
    )
    assert (
        format(
            "insert into x(a) ( with x as (select z from a) select x from y)",
            Dialect(Vendor.redshift),
            indent=8,
        )
        == """
        INSERT INTO x(a) (
            WITH x AS (
                SELECT z
                FROM a)
            SELECT x
            FROM y)
        """
    )
    assert (
        format("insert into x(a) select x from y")
        == "INSERT INTO x(a)\nSELECT x\nFROM y\n"
    )


def test_replace() -> None:
    assert format("replace into x(a) values(1)") == "REPLACE INTO x(a)\nVALUES (1)\n"
