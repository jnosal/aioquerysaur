from pathlib import Path
import pytest

from aioquerysaur import load_from_file, load_from_str, load_here
from aioquerysaur.exceptions import SQLLoadException
from aioquerysaur.queries import QueriesContainer


@pytest.fixture
def sql_file():
    return Path(__file__).parent / "sql/items.sql"


@pytest.fixture
def sql_dir():
    return Path(__file__).parent / "sql/dir"


@pytest.fixture
def sql():
    with open(Path(__file__).parent / "sql/items.sql") as f:
        return f.read()


@pytest.fixture
def invalid_sql():
    with open(Path(__file__).parent / "sql/items_invalid.sql") as f:
        return f.read()


def test_returns_zero_datum_objects_for_invalid_contents(invalid_sql):
    queries = load_from_str(invalid_sql, "psycopg2")
    assert len(queries.available) == 0


def test_load_from_str_correct_query_datum_when_no_record_class(sql):
    queries = load_from_str(sql, "psycopg2")

    assert isinstance(queries, QueriesContainer)
    assert len(queries.available) > 0


def test_raises_value_error_when_incorrect_backend(sql):
    with pytest.raises(ValueError):
        load_from_str(sql, "i-dont-exist")


def test_load_from_file_raises_exception_when_file_does_not_exist():
    with pytest.raises(SQLLoadException):
        load_from_file("i-dont-exist", "psycopg2")


def test_load_dir(mocker, sql_dir):
    mock_load_from_tree = mocker.patch(
        "aioquerysaur.queries.QueriesContainer.load_from_tree"
    )
    load_from_file(sql_dir, "psycopg2")
    mock_load_from_tree.assert_called_once()


def test_load_file(mocker, sql_file):
    mock_load_from_list = mocker.patch(
        "aioquerysaur.queries.QueriesContainer.load_from_list"
    )
    load_from_file(sql_file, "psycopg2")
    mock_load_from_list.assert_called_once()


def test_load_here(mocker):
    mock_load_from_file = mocker.patch("aioquerysaur.loader.load_from_file")
    load_here("psycopg2")
    args, kwargs = mock_load_from_file.call_args

    assert "sql_path" in kwargs
    assert kwargs["sql_path"] == Path.cwd()


def test_correct_names_are_made_for_loaded_queries(sql):
    queries = load_from_str(sql, "psycopg2")

    assert "get_list" in queries.available
    assert "get_list_alt" in queries.available
