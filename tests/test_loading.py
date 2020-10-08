from pathlib import Path
import pytest

from aioquerysaur import load_from_str
from aioquerysaur.queries import QueriesContainer


@pytest.fixture
def sql_dir():
    return Path(__file__).parent / "sql"


@pytest.fixture
def sql(sql_dir):
    with open(sql_dir / "items.sql") as f:
        return f.read()


@pytest.fixture
def invalid_sql(sql_dir):
    with open(sql_dir / "items_invalid.sql") as f:
        return f.read()


def test_returns_zero_datum_objects_for_invalid_file(invalid_sql):
    queries = load_from_str(invalid_sql, "psycopg2")
    assert len(queries.available) == 0


def test_load_from_str_correct_query_datum_when_no_record_class(sql):
    queries = load_from_str(sql, "psycopg2")

    assert isinstance(queries, QueriesContainer)
    assert len(queries.available) > 0


def test_correct_names_are_made(sql):
    queries = load_from_str(sql, "psycopg2")

    assert 'get_list' in queries.available
    assert 'get_list_alt' in queries.available
