from pathlib import Path
import sqlite3

import aiosqlite
import pytest


def popuate_db(db_path):
    conn = sqlite3.connect(db_path, uri=True)
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
    conn.close()


@pytest.fixture
def sqlite3_path(tmpdir):
    db_path = str(Path(tmpdir.strpath) / "aioquerysaur.db")
    popuate_db(db_path)
    return db_path


@pytest.fixture
def sqlite3_conn(sqlite3_path):
    conn = sqlite3.connect(sqlite3_path, uri=True)
    yield conn
    conn.close()


@pytest.fixture
async def aiosqlite_conn(sqlite3_path):
    conn = await aiosqlite.connect(sqlite3_path, uri=True)
    yield conn
    await conn.close()
