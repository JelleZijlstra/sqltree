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
    assert (
        format("select x from y where x = (select y from x)", indent=12)
        == """
            SELECT x
            FROM y
            WHERE x = (
                SELECT y
                FROM x)
        """
    )
    assert (
        format("select x from y where x in (select y from x)", indent=12)
        == """
            SELECT x
            FROM y
            WHERE x IN (
                SELECT y
                FROM x)
        """
    )
    assert (
        format("select x from y where x in (a, b, c)", indent=12)
        == """
            SELECT x
            FROM y
            WHERE x IN (a, b, c)
        """
    )
    assert (
        format("select x from y where x in {lst}", indent=12)
        == """
            SELECT x
            FROM y
            WHERE x IN {lst}
        """
    )
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
    assert format("select x where y is not null") == "SELECT x\nWHERE y IS NOT NULL\n"
    assert (
        format("select x where left(y, 5) = 'x'")
        == "SELECT x\nWHERE LEFT(y, 5) = 'x'\n"
    )

    assert format("select binary 'x'") == "SELECT BINARY 'x'\n"


def test_literals() -> None:
    assert format("select 'x'") == "SELECT 'x'\n"
    assert format('select "x"') == "SELECT 'x'\n"
    assert format("select 1") == "SELECT 1\n"
    assert format("select 1.0") == "SELECT 1.0\n"
    assert format("select 1.0e10") == "SELECT 1.0e10\n"


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
    assert (
        format(sql, indent=12)
        == """
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
    )


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

    assert (
        format("select x from a join b", indent=12)
        == """
            SELECT x
            FROM
                a
            JOIN
                b
        """
    )
    assert (
        format("select x from a join b join c", indent=12)
        == """
            SELECT x
            FROM
                a
            JOIN
                b
            JOIN
                c
        """
    )
    assert (
        format("select x from a join b on x = y join c on y = x", indent=12)
        == """
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
    )
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
