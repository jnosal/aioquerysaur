__version__ = "0.1.0"

from typing import Callable, Type, Union
from pathlib import Path
from typing import Dict

from .adapters.aiosqlite import AioSQLiteAdapter
from .adapters.sqlite3 import SQLite3DriverAdapter
from .adapters.psycopg2 import PsycoPG2Adapter
from .loaders.text import TextLoader
from .exceptions import SQLParseException, SQLLoadException
from .models import (
    DriverAdapterProtocol,
    FileQueryLoaderProtocol,
    TextQueryLoaderProtocol,
)
from .queries import QueriesContainer


_ADAPTERS: Dict[str, Callable[..., DriverAdapterProtocol]] = {
    "psycopg2": PsycoPG2Adapter,
    "sqlite3": SQLite3DriverAdapter,
    "aiosqlite": AioSQLiteAdapter,
}


_LOADERS: Dict[str, Callable[..., FileQueryLoaderProtocol]] = {
    "text": TextLoader,
}


def _make_driver_adapter_instance(
    driver_adapter: Union[str, Callable[..., DriverAdapterProtocol]]
) -> DriverAdapterProtocol:
    if isinstance(driver_adapter, str):
        try:
            driver_adapter = _ADAPTERS[driver_adapter]
        except KeyError:
            raise ValueError(
                f"Encountered unregistered driver_adapter: {driver_adapter}"
            )

    return driver_adapter()


def _make_loader_cls(
    loader: Union[str, Callable[..., FileQueryLoaderProtocol]]
) -> Union[Type[FileQueryLoaderProtocol], Type[TextQueryLoaderProtocol]]:
    if isinstance(loader, str):
        try:
            loader = _LOADERS[loader]
        except KeyError:
            raise ValueError(f"Encountered unregistered loader: {loader}")

    return loader


def load_from_str(
    sql: str,
    driver_adapter: Union[str, Callable[..., DriverAdapterProtocol]],
    loader_cls: Union[str, Callable[..., TextQueryLoaderProtocol]] = TextLoader,
    queries_cls: Type[QueriesContainer] = QueriesContainer,
):
    # initiate driver adapter from str or callable
    adapter = _make_driver_adapter_instance(driver_adapter)
    # create loader cls from str or type
    loader = _make_loader_cls(loader_cls)
    # initiate query loader
    query_loader = loader(adapter, record_classes=None)
    # load query data
    query_data = query_loader.load_query_data_from_sql(sql)
    return queries_cls(adapter).load_from_list(query_data)


def load_from_file(
    sql_path: Union[str, Path],
    driver_adapter: Union[str, Callable[..., DriverAdapterProtocol]],
    loader_cls: Union[str, Callable[..., FileQueryLoaderProtocol]] = TextLoader,
    queries_cls: Type[QueriesContainer] = QueriesContainer,
):
    path = Path(sql_path)

    if not path.exists():
        raise SQLLoadException(f"File does not exist: {path}")

    # initiate driver adapter from str or callable
    adapter = _make_driver_adapter_instance(driver_adapter)
    # create loader cls from str or type
    loader = _make_loader_cls(loader_cls)
    # initiate query loader
    query_loader = loader(adapter, record_classes=None)
    # load query data

    if path.is_file():
        query_data = query_loader.load_query_data_from_file(path)
        return queries_cls(adapter).load_from_list(query_data)
    elif path.is_dir():
        query_data_tree = query_loader.load_query_data_from_dir(path)
        return queries_cls(adapter).load_from_tree(query_data_tree)
    else:
        raise SQLLoadException(
            f"The sql_path must be a directory or file, got {sql_path}"
        )


def load_here(
    driver_adapter: Union[str, Callable[..., DriverAdapterProtocol]],
    loader_cls: Union[str, Callable[..., FileQueryLoaderProtocol]] = TextLoader,
    queries_cls: Type[QueriesContainer] = QueriesContainer,
):
    return load_from_file(
        sql_path=Path.cwd(),
        driver_adapter=driver_adapter,
        loader_cls=loader_cls,
        queries_cls=queries_cls,
    )


__all__ = [
    "load_from_str",
    "load_here",
    "load_from_file",
    "SQLLoadException",
    "SQLParseException",
]
