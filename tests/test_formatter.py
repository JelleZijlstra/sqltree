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
        format('select "FROM" from "SELECT"', Dialect(Vendor.ansi))
        == 'SELECT "FROM"\nFROM "SELECT"\n'
    )

    assert (
        format(
            (
                "select max(x) from y where x = some(function, many, args) and y ="
                " no_args()"
            ),
            indent=12,
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
            "select aaaaaa, bbbbbbbb, ccccc, dddddd from x", line_length=20, indent=12
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
        format(
            "select max(x) from y where x = 1 and y = 2 and z = 3 limit 4", indent=12
        )
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
    assert (
        format(
            "select max(x) from y where x = 1 and y = 2 or x = 2 and y = 1 limit 4",
            indent=12,
        )
        == """
            SELECT max(x)
            FROM y
            WHERE
                    x = 1
                    AND y = 2
                OR
                    x = 2
                    AND y = 1
            LIMIT 4
        """
    )
    assert (
        format(
            "select max(x) from y where (x = 1 or y = 2) and z = 3 limit 4", indent=12
        )
        == """
            SELECT max(x)
            FROM y
            WHERE
                (
                    x = 1
                    OR y = 2
                )
                AND z = 3
            LIMIT 4
        """
    )
    assert format("select x from y {limit}") == "SELECT x\nFROM y\n{limit}\n"
    assert (
        format("select x from y order by z {limit}")
        == "SELECT x\nFROM y\nORDER BY z\n{limit}\n"
    )
    assert (
        format("select x from `y` where `select` = 3")
        == "SELECT x\nFROM y\nWHERE `select` = 3\n"
    )
    assert (
        format('select x from "y" where "select" = 3', Dialect(Vendor.redshift))
        == 'SELECT x\nFROM y\nWHERE "select" = 3\n'
    )
    assert format("select distinct x from y") == "SELECT DISTINCT x\nFROM y\n"
    assert (
        format("select all high_priority sql_no_cache x from y")
        == "SELECT ALL HIGH_PRIORITY SQL_NO_CACHE x\nFROM y\n"
    )
    assert format("select x from y where x = (select y from x)", indent=12) == """
            SELECT x
            FROM y
            WHERE x = (
                SELECT y
                FROM x)
        """
    assert format("select x from y where x in (select y from x)", indent=12) == """
            SELECT x
            FROM y
            WHERE x IN (
                SELECT y
                FROM x)
        """
    assert format("select x from y where x in (a, b, c)", indent=12) == """
            SELECT x
            FROM y
            WHERE x IN (a, b, c)
        """
    assert format("select x from y where x in {lst}", indent=12) == """
            SELECT x
            FROM y
            WHERE x IN {lst}
        """
    assert (
        format("select case x when y then z else alpha end")
        == "SELECT CASE x WHEN y THEN z ELSE alpha END\n"
    )
    assert (
        format("select case when y then z when alpha then beta end from gamma")
        == "SELECT CASE WHEN y THEN z WHEN alpha THEN beta END\nFROM gamma\n"
    )
    assert (
        format("select x from y where x.y = 3") == "SELECT x\nFROM y\nWHERE x.y = 3\n"
    )
    assert (
        format("select y.x from x.y where x.y = 3")
        == "SELECT y.x\nFROM x.y\nWHERE x.y = 3\n"
    )
    assert (
        format("select x from y where y = 'x'") == "SELECT x\nFROM y\nWHERE y = 'x'\n"
    )
    assert format("select a.* from a, b") == "SELECT a.*\nFROM a, b\n"

    assert format("select a from b for update") == "SELECT a\nFROM b\nFOR UPDATE\n"
    assert (
        format("select a from b for update skip locked")
        == "SELECT a\nFROM b\nFOR UPDATE SKIP LOCKED\n"
    )
    assert format("select 1 as `x y`") == "SELECT 1 AS `x y`\n"


def test_select_into() -> None:
    # locations for INTO
    assert format("select 1 into outfile 'x'") == "SELECT 1\nINTO OUTFILE 'x'\n"
    assert (
        format("select 1 into outfile 'x' from x")
        == "SELECT 1\nINTO OUTFILE 'x'\nFROM x\n"
    )
    assert (
        format("select 1 from x into outfile 'x' for update")
        == "SELECT 1\nFROM x\nINTO OUTFILE 'x'\nFOR UPDATE\n"
    )
    assert (
        format("select 1 from x for update into outfile 'x'")
        == "SELECT 1\nFROM x\nFOR UPDATE\nINTO OUTFILE 'x'\n"
    )

    # INTO DUMPFILE
    assert format("select 1 into dumpfile 'x'") == "SELECT 1\nINTO DUMPFILE 'x'\n"

    # INTO OUTFILE
    assert (
        format("select 1 into outfile 'x' character set ascii")
        == "SELECT 1\nINTO OUTFILE 'x' CHARACTER SET ascii\n"
    )
    assert (
        format(
            "select 1 into outfile 'x' character set ascii fields terminated by 'x'"
            " enclosed by 'y' escaped by 'z' lines starting by 'alpha' terminated by"
            " 'beta'"
        )
        == "SELECT 1\nINTO OUTFILE 'x' CHARACTER SET ascii FIELDS TERMINATED BY 'x'"
        " ENCLOSED BY 'y' ESCAPED BY 'z' LINES STARTING BY 'alpha' TERMINATED BY"
        " 'beta'\n"
    )
    assert (
        format(
            "select 1 into outfile 'x' character set ascii columns terminated by 'x'"
            " enclosed by 'y' escaped by 'z' lines starting by 'alpha' terminated by"
            " %s"
        )
        == "SELECT 1\nINTO OUTFILE 'x' CHARACTER SET ascii COLUMNS TERMINATED BY 'x'"
        " ENCLOSED BY 'y' ESCAPED BY 'z' LINES STARTING BY 'alpha' TERMINATED BY"
        " %s\n"
    )


def test_count() -> None:
    # TODO: maybe uppercase COUNT
    assert format("select count(*) from x") == "SELECT count(*)\nFROM x\n"
    assert (
        format("select count(*) from x where y = 3")
        == "SELECT count(*)\nFROM x\nWHERE y = 3\n"
    )
    assert format("select count(a) from b") == "SELECT count(a)\nFROM b\n"
    assert (
        format("select count(distinct a) from b")
        == "SELECT count(DISTINCT a)\nFROM b\n"
    )


def test_expression():
    assert (
        format("select x from y where x = y + 1")
        == "SELECT x\nFROM y\nWHERE x = y + 1\n"
    )
    assert format("select x from y where x = -1") == "SELECT x\nFROM y\nWHERE x = -1\n"
    assert (
        format("select -(select x from y)") == "SELECT -(\n    SELECT x\n    FROM y)\n"
    )
    assert format("select -1 + 1") == "SELECT -1 + 1\n"
    assert format("select -~1") == "SELECT -(~1)\n"
    assert format("select x where y is null") == "SELECT x\nWHERE y IS NULL\n"
    assert format("select x where not y is null") == "SELECT x\nWHERE NOT y IS NULL\n"
    assert format("select x where y is not null") == "SELECT x\nWHERE y IS NOT NULL\n"
    assert (
        format("select x where left(y, 5) = 'x'")
        == "SELECT x\nWHERE LEFT(y, 5) = 'x'\n"
    )

    assert format("select 2 > 1") == "SELECT 2 > 1\n"
    assert format("select 2 % 1") == "SELECT 2 % 1\n"
    # We allow this in conjunction with %s substitution.
    assert format("select 2 %% 1") == "SELECT 2 %% 1\n"

    assert format("select binary 'x'") == "SELECT BINARY 'x'\n"

    assert format("select a mod b") == "SELECT a MOD b\n"
    assert format("select a div b") == "SELECT a DIV b\n"


def test_cast() -> None:
    assert format("select cast(1 as binary)") == "SELECT CAST(1 AS BINARY)\n"
    assert format("select cast(1 as binary(5))") == "SELECT CAST(1 AS BINARY(5))\n"
    assert (
        format("select cast(1 as binary(5) array)")
        == "SELECT CAST(1 AS BINARY(5) ARRAY)\n"
    )
    assert format("select cast(1 as char)") == "SELECT CAST(1 AS CHAR)\n"
    assert format("select cast(1 as char(5))") == "SELECT CAST(1 AS CHAR(5))\n"
    assert format("select cast(1 as char ascii)") == "SELECT CAST(1 AS CHAR ASCII)\n"
    assert (
        format("select cast(1 as char unicode)") == "SELECT CAST(1 AS CHAR UNICODE)\n"
    )
    assert (
        format("select cast(1 as char(5) ascii)") == "SELECT CAST(1 AS CHAR(5) ASCII)\n"
    )
    assert (
        format("select cast(1 as char(5) character set latin1)")
        == "SELECT CAST(1 AS CHAR(5) CHARACTER SET latin1)\n"
    )
    assert (
        format("select cast(1 as char(5) character set 'latin1')")
        == "SELECT CAST(1 AS CHAR(5) CHARACTER SET 'latin1')\n"
    )
    assert format("select cast(1 as date)") == "SELECT CAST(1 AS DATE)\n"
    assert format("select cast(1 as datetime)") == "SELECT CAST(1 AS DATETIME)\n"
    assert format("select cast(1 as datetime(6))") == "SELECT CAST(1 AS DATETIME(6))\n"
    assert format("select cast(1 as decimal(6))") == "SELECT CAST(1 AS DECIMAL(6))\n"
    assert (
        format("select cast(1 as decimal(6, 7))") == "SELECT CAST(1 AS DECIMAL(6, 7))\n"
    )
    assert format("select cast(1 as double)") == "SELECT CAST(1 AS DOUBLE)\n"
    assert format("select cast(1 as float)") == "SELECT CAST(1 AS FLOAT)\n"
    assert format("select cast(1 as float(3))") == "SELECT CAST(1 AS FLOAT(3))\n"
    assert format("select cast(1 as json)") == "SELECT CAST(1 AS JSON)\n"
    assert format("select cast(1 as nchar)") == "SELECT CAST(1 AS NCHAR)\n"
    assert format("select cast(1 as nchar(5))") == "SELECT CAST(1 AS NCHAR(5))\n"
    assert format("select cast(1 as real)") == "SELECT CAST(1 AS REAL)\n"
    assert format("select cast(1 as signed)") == "SELECT CAST(1 AS SIGNED)\n"
    assert (
        format("select cast(1 as signed integer)")
        == "SELECT CAST(1 AS SIGNED INTEGER)\n"
    )
    assert format("select cast(1 as time)") == "SELECT CAST(1 AS TIME)\n"
    assert format("select cast(1 as time(6))") == "SELECT CAST(1 AS TIME(6))\n"
    assert format("select cast(1 as unsigned)") == "SELECT CAST(1 AS UNSIGNED)\n"
    assert (
        format("select cast(1 as unsigned integer)")
        == "SELECT CAST(1 AS UNSIGNED INTEGER)\n"
    )
    assert format("select cast(1 as year)") == "SELECT CAST(1 AS YEAR)\n"
    assert (
        format("select cast(1 as bigint)", Dialect(Vendor.redshift))
        == "SELECT CAST(1 AS BIGINT)\n"
    )
    assert (
        format("select cast(1 as bigint)", Dialect(Vendor.trino))
        == "SELECT CAST(1 AS BIGINT)\n"
    )
    assert (
        format(
            "select cast('1997-12-17 07:37:16-08'as TIME WITH TIME ZONE)",
            Dialect(Vendor.redshift),
        )
        == "SELECT CAST('1997-12-17 07:37:16-08' AS TIME WITH TIME ZONE)\n"
    )


def test_colon_cast() -> None:
    assert format("select 1 ::binary", Dialect(Vendor.redshift)) == "SELECT 1::BINARY\n"
    assert (
        format("select 1::binary(5)", Dialect(Vendor.redshift))
        == "SELECT 1::BINARY(5)\n"
    )
    assert format("select 1::char", Dialect(Vendor.redshift)) == "SELECT 1::CHAR\n"
    assert (
        format("select 1::char(5)", Dialect(Vendor.redshift)) == "SELECT 1::CHAR(5)\n"
    )
    assert format("select 1 :: date", Dialect(Vendor.redshift)) == "SELECT 1::DATE\n"
    assert (
        format("select 1 ::datetime", Dialect(Vendor.redshift))
        == "SELECT 1::DATETIME\n"
    )
    assert (
        format("select 1 :: datetime(6)", Dialect(Vendor.redshift))
        == "SELECT 1::DATETIME(6)\n"
    )
    assert (
        format("select 1 :: decimal(6)", Dialect(Vendor.redshift))
        == "SELECT 1::DECIMAL(6)\n"
    )
    assert (
        format("select 1 :: decimal(6, 7)", Dialect(Vendor.redshift))
        == "SELECT 1::DECIMAL(6, 7)\n"
    )
    assert (
        format("select 1 :: double", Dialect(Vendor.redshift)) == "SELECT 1::DOUBLE\n"
    )
    assert format("select 1 :: float", Dialect(Vendor.redshift)) == "SELECT 1::FLOAT\n"
    assert (
        format("select 1 :: float(3)", Dialect(Vendor.redshift))
        == "SELECT 1::FLOAT(3)\n"
    )
    assert format("select 1 :: nchar", Dialect(Vendor.redshift)) == "SELECT 1::NCHAR\n"
    assert (
        format("select 1 :: nchar(5)", Dialect(Vendor.redshift))
        == "SELECT 1::NCHAR(5)\n"
    )
    assert format("select 1 :: real", Dialect(Vendor.redshift)) == "SELECT 1::REAL\n"
    assert format("select 1 :: year", Dialect(Vendor.redshift)) == "SELECT 1::YEAR\n"
    assert (
        format("select 1 :: bigint", Dialect(Vendor.redshift)) == "SELECT 1::BIGINT\n"
    )
    assert (
        format("select 1 :: double PRECISION", Dialect(Vendor.redshift))
        == "SELECT 1::DOUBLE PRECISION\n"
    )
    assert (
        format(
            "select '1997-12-17 07:37:16-08':: TIME WITH TIME  zone",
            Dialect(Vendor.redshift),
        )
        == "SELECT '1997-12-17 07:37:16-08'::TIME WITH TIME ZONE\n"
    )
    assert (
        format("select '1' :: double", Dialect(Vendor.redshift))
        == "SELECT '1'::DOUBLE\n"
    )
    assert (
        format("select (1) :: double", Dialect(Vendor.redshift))
        == "SELECT (1)::DOUBLE\n"
    )


def test_typed_string() -> None:
    assert format("select DATE'2023-01-01'") == "SELECT DATE'2023-01-01'\n"
    assert format("select time'00:00:00'") == "SELECT TIME'00:00:00'\n"
    assert (
        format("select decimal(6)'1'", Dialect(Vendor.redshift))
        == "SELECT DECIMAL(6)'1'\n"
    )

    assert format("select float '1'", Dialect(Vendor.redshift)) == "SELECT FLOAT'1'\n"
    assert format("select real '1'", Dialect(Vendor.trino)) == "SELECT REAL'1'\n"
    assert format("select bigint '1'", Dialect(Vendor.trino)) == "SELECT BIGINT'1'\n"
    assert (
        format("select datetime(6)'1'", Dialect(Vendor.redshift))
        == "SELECT DATETIME(6)'1'\n"
    )
    assert (
        format("select double PRECISION '1' ", Dialect(Vendor.redshift))
        == "SELECT DOUBLE PRECISION'1'\n"
    )
    assert (
        format(
            "select TIME WITH TIME  zone '1997-12-17 07:37:16-08'",
            Dialect(Vendor.redshift),
        )
        == "SELECT TIME WITH TIME ZONE'1997-12-17 07:37:16-08'\n"
    )
    assert format("select double '1'", Dialect(Vendor.redshift)) == "SELECT DOUBLE'1'\n"
    assert format("select nchar '1'", Dialect(Vendor.redshift)) == "SELECT NCHAR'1'\n"
    assert (
        format("select nchar(5)'1'", Dialect(Vendor.redshift)) == "SELECT NCHAR(5)'1'\n"
    )


def test_group_concat() -> None:
    assert format("select group_concat(a)") == "SELECT GROUP_CONCAT(a)\n"
    assert (
        format("select group_concat(distinct a)") == "SELECT GROUP_CONCAT(DISTINCT a)\n"
    )
    assert (
        format("select group_concat(distinct a, b)")
        == "SELECT GROUP_CONCAT(DISTINCT a, b)\n"
    )
    assert (
        format("select group_concat(distinct a order by b)")
        == "SELECT GROUP_CONCAT(DISTINCT a ORDER BY b)\n"
    )
    assert (
        format('select group_concat(a separator ",")')
        == "SELECT GROUP_CONCAT(a SEPARATOR ',')\n"
    )


def test_aggregate_functtions() -> None:
    assert format("select avg(x)") == "SELECT avg(x)\n"
    assert format("select avg(distinct x)") == "SELECT avg(DISTINCT x)\n"
    assert format("select bit_and(x)") == "SELECT bit_and(x)\n"
    assert format("select bit_or(x)") == "SELECT bit_or(x)\n"
    assert format("select bit_xor(x)") == "SELECT bit_xor(x)\n"
    assert format("select count(x)") == "SELECT count(x)\n"
    assert format("select count(distinct x)") == "SELECT count(DISTINCT x)\n"
    assert format("select json_arrayagg(x)") == "SELECT json_arrayagg(x)\n"
    assert format("select json_objectagg(x, y)") == "SELECT json_objectagg(x, y)\n"
    assert format("select max(x)") == "SELECT max(x)\n"
    assert format("select max(distinct x)") == "SELECT max(DISTINCT x)\n"
    assert format("select min(x)") == "SELECT min(x)\n"
    # Why does MIN() support DISTINCT even though it doesn't do anything,
    # but STDDEV doesn't?
    assert format("select min(distinct x)") == "SELECT min(DISTINCT x)\n"
    assert format("select std(x)") == "SELECT std(x)\n"
    assert format("select stddev(x)") == "SELECT stddev(x)\n"
    assert format("select stddev_pop(x)") == "SELECT stddev_pop(x)\n"
    assert format("select stddev_samp(x)") == "SELECT stddev_samp(x)\n"
    assert format("select sum(x)") == "SELECT sum(x)\n"
    assert format("select sum(distinct x)") == "SELECT sum(DISTINCT x)\n"
    assert format("select var_pop(x)") == "SELECT var_pop(x)\n"
    assert format("select var_samp(x)") == "SELECT var_samp(x)\n"
    assert format("select variance(x)") == "SELECT variance(x)\n"


def test_literals() -> None:
    assert format("select 'x'") == "SELECT 'x'\n"
    assert format("select '''x'") == "SELECT '''x'\n"
    assert format('select "x"') == "SELECT 'x'\n"
    assert format("select 1") == "SELECT 1\n"
    assert format("select 1.0") == "SELECT 1.0\n"
    assert format("select 1.0e10") == "SELECT 1.0e10\n"
    assert format("select 1.0E10") == "SELECT 1.0e10\n"
    assert format("select 1.0e-10") == "SELECT 1.0e-10\n"
    assert format("select 1E-2 ") == "SELECT 1e-2\n"


def test_union() -> None:
    assert (
        format("select x from y union select x from z")
        == "SELECT x\nFROM y\nUNION\nSELECT x\nFROM z\n"
    )
    assert (
        format("select x from y union all select x from z")
        == "SELECT x\nFROM y\nUNION ALL\nSELECT x\nFROM z\n"
    )
    assert (
        format("select x from y union distinct select x from z")
        == "SELECT x\nFROM y\nUNION DISTINCT\nSELECT x\nFROM z\n"
    )
    assert (
        format(
            "select x from y union distinct select x from z union all select a from b"
        )
        == "SELECT x\nFROM y\nUNION DISTINCT\nSELECT x\nFROM z\nUNION ALL\nSELECT"
        " a\nFROM b\n"
    )
    assert format("(select x from y)") == "(\n    SELECT x\n    FROM y)\n"
    assert (
        format("(select x from y) union (select x from z)")
        == "(\n    SELECT x\n    FROM y)\nUNION\n(\n    SELECT x\n    FROM z)\n"
    )
    assert (
        format("(select x from y) union (select x from z) order by x")
        == "(\n    SELECT x\n    FROM y)\nUNION\n(\n    SELECT x\n    FROM z)\nORDER"
        " BY x\n"
    )


def test_multi_split() -> None:
    sql = """
        SELECT
        ghi,
        COUNT(CASE WHEN result = %s THEN 1 END) as count,
        COUNT(CASE WHEN result = %s THEN 1 END) as c2
        FROM jkl
        WHERE abc = %s
        AND time >= %s
        GROUP BY def
    """
    assert format(sql, indent=12) == """
            SELECT
                ghi,
                COUNT(CASE WHEN result = %s THEN 1 END) AS count,
                COUNT(CASE WHEN result = %s THEN 1 END) AS c2
            FROM jkl
            WHERE
                abc = %s
                AND time >= %s
            GROUP BY def
        """


def test_table_reference() -> None:
    assert format("select x from y use index(z)") == "SELECT x\nFROM y\nUSE INDEX(z)\n"
    assert (
        format("select x from y use index(PRIMARY)")
        == "SELECT x\nFROM y\nUSE INDEX(PRIMARY)\n"
    )
    assert (
        format("select x from y use index(z), ignore key for join(z)")
        == "SELECT x\nFROM y\nUSE INDEX(z),\nIGNORE KEY FOR JOIN(z)\n"
    )
    assert (
        format("select x from y use index(z), ignore key for order by   (z)")
        == "SELECT x\nFROM y\nUSE INDEX(z),\nIGNORE KEY FOR ORDER BY(z)\n"
    )
    assert format("select x from y use index()") == "SELECT x\nFROM y\nUSE INDEX()\n"
    with pytest.raises(ParseError):
        format("select x from y force index()")

    assert format("select x from (a, b)") == "SELECT x\nFROM (a, b)\n"
    assert format("select x from a, b") == "SELECT x\nFROM a, b\n"

    assert format("select x from a join b", indent=12) == """
            SELECT x
            FROM
                a
            JOIN
                b
        """
    assert format("select x from a left join b on 1", indent=12) == """
            SELECT x
            FROM
                a
            LEFT JOIN
                b
            ON 1
        """
    assert format("select x from a cross join b", indent=12) == """
            SELECT x
            FROM
                a
            CROSS JOIN
                b
        """
    assert format("select x from a join b join c", indent=12) == """
            SELECT x
            FROM
                a
            JOIN
                b
            JOIN
                c
        """
    assert format("select x from a join b on x = y join c on y = x", indent=12) == """
            SELECT x
            FROM
                a
            JOIN
                b
            ON x = y
            JOIN
                c
            ON y = x
        """
    assert (
        format("select x from a join b on x = y join c on y = x and c = d", indent=12)
        == """
            SELECT x
            FROM
                a
            JOIN
                b
            ON x = y
            JOIN
                c
            ON
                y = x
                AND c = d
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
            indent=12,
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
            indent=12,
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
    assert (
        format(
            "insert into x(a) values(1) on duplicate key update a = values(a)",
            indent=12,
        )
        == """
            INSERT INTO x(a)
            VALUES (1)
            ON DUPLICATE KEY UPDATE a = VALUES(a)
        """
    )
    assert (
        format("INSERT INTO x(a, b) values(1, null)")
        == "INSERT INTO x(a, b)\nVALUES (1, NULL)\n"
    )


def test_replace() -> None:
    assert format("replace into x(a) values(1)") == "REPLACE INTO x(a)\nVALUES (1)\n"


def test_start_transaction() -> None:
    assert format("start transaction") == "START TRANSACTION\n"
    assert (
        format("start transaction with consistent snapshot")
        == "START TRANSACTION WITH CONSISTENT SNAPSHOT\n"
    )
    assert (
        format("start transaction with consistent snapshot, read write")
        == "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ WRITE\n"
    )


def test_begin() -> None:
    assert format("begin") == "BEGIN\n"
    assert format("begin work") == "BEGIN WORK\n"


def test_commit() -> None:
    assert format("commit") == "COMMIT\n"
    assert format("commit work") == "COMMIT WORK\n"
    assert format("commit and no chain release") == "COMMIT AND NO CHAIN RELEASE\n"
    assert format("commit and chain no release") == "COMMIT AND CHAIN NO RELEASE\n"


def test_rollback() -> None:
    assert format("rollback") == "ROLLBACK\n"
    assert format("rollback work") == "ROLLBACK WORK\n"
    assert format("rollback and no chain release") == "ROLLBACK AND NO CHAIN RELEASE\n"
    assert format("rollback and chain no release") == "ROLLBACK AND CHAIN NO RELEASE\n"


def test_set_transaction() -> None:
    assert (
        format("set transaction isolation level repeatable read")
        == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ\n"
    )
    assert (
        format("set global transaction isolation level repeatable read")
        == "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ\n"
    )
    assert (
        format("set session transaction isolation level repeatable read")
        == "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ\n"
    )
    assert (
        format("set transaction isolation level repeatable read, read write")
        == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE\n"
    )


def test_drop_table() -> None:
    assert format("drop table x") == "DROP TABLE x\n"
    assert (
        format("drop temporary table if exists x")
        == "DROP TEMPORARY TABLE IF EXISTS x\n"
    )
    assert format("drop table x cascade") == "DROP TABLE x CASCADE\n"
    assert format("drop table x restrict") == "DROP TABLE x RESTRICT\n"
    assert format("drop table x.y, a.b") == "DROP TABLE x.y, a.b\n"


def test_show_tables() -> None:
    assert format("show tables") == "SHOW TABLES\n"
    assert (
        format("show extended full tables from x")
        == "SHOW EXTENDED FULL TABLES FROM x\n"
    )
    assert format("show tables in x") == "SHOW TABLES IN x\n"
    assert format("show tables in x like 'y'") == "SHOW TABLES IN x\nLIKE 'y'\n"
    assert format("show tables in x where z = 3") == "SHOW TABLES IN x\nWHERE z = 3\n"
    assert format("show tables like %s") == "SHOW TABLES\nLIKE %s\n"


def test_show_columns() -> None:
    assert (
        format("show extended full columns from x")
        == "SHOW EXTENDED FULL COLUMNS FROM x\n"
    )
    assert format("show columns in x") == "SHOW COLUMNS IN x\n"
    assert format("show columns in x from y") == "SHOW COLUMNS IN x FROM y\n"
    assert format("show columns in x like 'y'") == "SHOW COLUMNS IN x\nLIKE 'y'\n"
    assert format("show columns in x where z = 3") == "SHOW COLUMNS IN x\nWHERE z = 3\n"
    assert format("show fields in x where z = 3") == "SHOW FIELDS IN x\nWHERE z = 3\n"


def test_show_index() -> None:
    assert format("show extended index from x") == "SHOW EXTENDED INDEX FROM x\n"
    assert format("show index in x") == "SHOW INDEX IN x\n"
    assert format("show index in x from y") == "SHOW INDEX IN x FROM y\n"
    assert format("show index in x where z = 3") == "SHOW INDEX IN x\nWHERE z = 3\n"
    assert format("show indexes in x where z = 3") == "SHOW INDEXES IN x\nWHERE z = 3\n"
    assert format("show keys in x where z = 3") == "SHOW KEYS IN x\nWHERE z = 3\n"


def test_show_triggers() -> None:
    assert format("show triggers") == "SHOW TRIGGERS\n"
    assert format("show triggers in x") == "SHOW TRIGGERS IN x\n"
    assert format("show triggers in x like 'y'") == "SHOW TRIGGERS IN x\nLIKE 'y'\n"
    assert (
        format("show triggers in x where z = 3") == "SHOW TRIGGERS IN x\nWHERE z = 3\n"
    )


def test_show_table_status() -> None:
    assert format("show table status") == "SHOW TABLE STATUS\n"
    assert format("show table status in x") == "SHOW TABLE STATUS IN x\n"
    assert (
        format("show table status in x like 'y'")
        == "SHOW TABLE STATUS IN x\nLIKE 'y'\n"
    )
    assert (
        format("show table status in x where z = 3")
        == "SHOW TABLE STATUS IN x\nWHERE z = 3\n"
    )


def test_show_replica_status() -> None:
    assert format("show replica status") == "SHOW REPLICA STATUS\n"
    assert format("show slave status") == "SHOW SLAVE STATUS\n"
    assert (
        format("show replica status for channel 'x'")
        == "SHOW REPLICA STATUS FOR CHANNEL 'x'\n"
    )

    assert format("show slave hosts") == "SHOW SLAVE HOSTS\n"
    assert format("show replicas") == "SHOW REPLICAS\n"


def test_show_variables() -> None:
    assert format("show variables") == "SHOW VARIABLES\n"
    assert (
        format("show global variables where x = 3")
        == "SHOW GLOBAL VARIABLES\nWHERE x = 3\n"
    )
    assert (
        format("show session variables like 'x'")
        == "SHOW SESSION VARIABLES\nLIKE 'x'\n"
    )


def test_show_status() -> None:
    assert format("show status") == "SHOW STATUS\n"
    assert (
        format("show global status where x = 3") == "SHOW GLOBAL STATUS\nWHERE x = 3\n"
    )
    assert format("show session status like 'x'") == "SHOW SESSION STATUS\nLIKE 'x'\n"


def test_show_warnings() -> None:
    assert format("show warnings") == "SHOW WARNINGS\n"
    assert format("show errors") == "SHOW ERRORS\n"
    assert format("show count(*) warnings") == "SHOW COUNT(*) WARNINGS\n"
    assert format("show warnings limit 3") == "SHOW WARNINGS\nLIMIT 3\n"
    assert format("show warnings limit 1, 3") == "SHOW WARNINGS\nLIMIT 3 OFFSET 1\n"


def test_explain() -> None:
    assert format("explain select 1") == "EXPLAIN\nSELECT 1\n"
    assert format("explain select 1 from x") == "EXPLAIN\nSELECT 1\nFROM x\n"
    assert format("describe select 1") == "DESCRIBE\nSELECT 1\n"
    assert (
        format("explain format = tree select 1") == "EXPLAIN FORMAT = TREE\nSELECT 1\n"
    )


def test_flush() -> None:
    assert format("flush local binary logs") == "FLUSH LOCAL BINARY LOGS\n"
    assert (
        format("flush no_write_to_binlog binary logs")
        == "FLUSH NO_WRITE_TO_BINLOG BINARY LOGS\n"
    )
    assert format("flush binary logs") == "FLUSH BINARY LOGS\n"
    assert format("flush engine logs") == "FLUSH ENGINE LOGS\n"
    assert format("flush error logs") == "FLUSH ERROR LOGS\n"
    assert format("flush gENeRal logs") == "FLUSH GENERAL LOGS\n"
    assert format("flush hosts") == "FLUSH HOSTS\n"
    assert format("flush logs") == "FLUSH LOGS\n"
    assert format("flush optimizer_costs") == "FLUSH OPTIMIZER_COSTS\n"
    assert format("flush privileges") == "FLUSH PRIVILEGES\n"
    assert format("flush relay logs") == "FLUSH RELAY LOGS\n"
    assert (
        format("flush relay logs for channel 'hello'")
        == "FLUSH RELAY LOGS FOR CHANNEL 'hello'\n"
    )
    assert format("flush slow logs") == "FLUSH SLOW LOGS\n"
    assert format("flush status") == "FLUSH STATUS\n"
    assert format("flush user_resources") == "FLUSH USER_RESOURCES\n"
    assert (
        format("flush hosts, logs, relay logs for channel 'x', optimizer_costs")
        == "FLUSH HOSTS, LOGS, RELAY LOGS FOR CHANNEL 'x', OPTIMIZER_COSTS\n"
    )

    assert format("flush tables") == "FLUSH TABLES\n"
    assert format("flush tables x") == "FLUSH TABLES x\n"
    assert format("flush tables x, y") == "FLUSH TABLES x, y\n"
    assert format("flush tables with read lock") == "FLUSH TABLES WITH READ LOCK\n"
    assert format("flush tables x with read lock") == "FLUSH TABLES x WITH READ LOCK\n"
    assert (
        format("flush tables x, y with read lock")
        == "FLUSH TABLES x, y WITH READ LOCK\n"
    )
    assert format("flush tables x, y for export") == "FLUSH TABLES x, y FOR EXPORT\n"


def test_truncate() -> None:
    assert format("truncate table x") == "TRUNCATE TABLE x\n"
    assert format("truncate x") == "TRUNCATE x\n"
    assert format("truncate x.y") == "TRUNCATE x.y\n"


def test_create_table() -> None:
    assert format("create table x like y") == "CREATE TABLE x LIKE y\n"
    assert format("create table x (like y)") == "CREATE TABLE x (LIKE y)\n"
    assert format("create table a.b (like c.d)") == "CREATE TABLE a.b (LIKE c.d)\n"


def test_rename_tables() -> None:
    assert format("rename table x to y") == "RENAME TABLE x TO y\n"
    assert format("rename table x to y, z to w") == "RENAME TABLE x TO y, z TO w\n"


def test_placeholder() -> None:
    assert format("select * from x where x = ?") == "SELECT *\nFROM x\nWHERE x = ?\n"
    assert format("select * from x where x = %s") == "SELECT *\nFROM x\nWHERE x = %s\n"
    assert (
        format("select * from x where x = %(x)s")
        == "SELECT *\nFROM x\nWHERE x = %(x)s\n"
    )
