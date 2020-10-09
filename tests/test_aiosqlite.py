import pytest


@pytest.fixture
def aiosqlite_path():
    return "sqlite:///:memory:"
