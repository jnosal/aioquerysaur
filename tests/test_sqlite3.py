from pathlib import Path

import pytest
from aioquerysaur import load_from_str


@pytest.fixture
def sql():
    with open(Path(__file__).parent / "sql/items.sql") as f:
        return f.read()


def test_connect_to_sqlite_db_and_select_items(sql, sqlite3_conn):
    queries = load_from_str(sql, "sqlite3")

    with sqlite3_conn:
        items1 = queries.get_list(conn=sqlite3_conn, flag=False)
        assert len(items1) == 0

        items1 = queries.get_list(conn=sqlite3_conn, flag=True)
        assert len(items1) == 1
