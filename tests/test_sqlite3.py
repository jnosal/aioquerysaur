from pathlib import Path
import sqlite3

import pytest
from aioquerysaur import load_from_str


def popuate_db(conn):
    cur = conn.cursor()
    cur.executescript(
        """
            create table items (
                id integer not null primary key,
                title text not null,
                flag boolean,
                revealed boolean
            );
            insert into items(id, title, flag, revealed) values(1, 'a', 1, 1);
            """
    )
    conn.commit()


@pytest.fixture
def sqlite3_path():
    return "file::memory:?cache=shared"


@pytest.fixture
def sqlite3_conn(sqlite3_path):
    conn = sqlite3.connect(sqlite3_path, uri=True)
    popuate_db(conn)
    yield conn
    conn.close()


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
