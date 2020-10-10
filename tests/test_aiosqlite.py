from pathlib import Path

import pytest
from aioquerysaur import load_from_str


@pytest.fixture
def sql():
    with open(Path(__file__).parent / "sql/items.sql") as f:
        return f.read()


@pytest.mark.asyncio
async def test_connect_to_aiosqlite_db_and_select_items(sql, aiosqlite_conn):
    queries = load_from_str(sql, "aiosqlite")

    items1 = await queries.get_list(conn=aiosqlite_conn, flag=False)
    assert len(items1) == 0

    items1 = await queries.get_list(conn=aiosqlite_conn, flag=True)
    assert len(items1) == 1


@pytest.mark.asyncio
async def test_connect_to_aiosqlite_db_and_select_items(sql, aiosqlite_conn):
    queries = load_from_str(sql, "aiosqlite")

    items1 = await queries.get_list(conn=aiosqlite_conn, flag=False)
    assert len(items1) == 0

    await queries.create_item(
        conn=aiosqlite_conn, flag=False, revealed=False, title="bsd"
    )
    items1 = await queries.get_list(conn=aiosqlite_conn, flag=False)
    assert len(items1) == 1
