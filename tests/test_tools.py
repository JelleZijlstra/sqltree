from sqltree.tools import get_tables


def test_get_tables() -> None:
    assert get_tables("SELECT * FROM x WHERE a = 3") == ["x"]
    assert get_tables("SELECT * FROM x, y WHERE a = 3") == ["x", "y"]
    assert get_tables("SELECT * FROM x JOIN y ON x.a = y.b") == ["x", "y"]
    assert get_tables("SELECT * FROM x WHERE y in (SELECT y FROM z)") == ["x", "z"]
    assert get_tables("UPDATE x SET a = 3 WHERE y in (SELECT y FROM z)") == ["x", "z"]
    assert get_tables("DELETE FROM x WHERE y in (SELECT y FROM z)") == ["x", "z"]
    assert get_tables("INSERT INTO x (a, b) VALUES (1, 2)") == ["x"]
    assert get_tables("INSERT INTO x (a, b) SELECT * FROM y") == ["x", "y"]
    assert get_tables("REPLACE INTO x (a, b) SELECT * FROM y") == ["x", "y"]
    assert get_tables("SHOW COLUMNS FROM x") == ["x"]
    assert get_tables("SHOW INDEXES FROM `x`.`y`") == ["x.y"]
