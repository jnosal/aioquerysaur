from enum import Enum
from typing import Callable, Dict, Optional, Type, Union
import re
from pathlib import Path
from typing import Dict, List, Optional

from .adapters.psycopg2 import PsycoPG2Adapter
from .exceptions import SQLParseException, SQLLoadException
from .models import QueryDatum, QueryDataTree, SQLOperationType, DriverAdapterProtocol
from .queries import QueriesContainer


_ADAPTERS: Dict[str, Callable[..., DriverAdapterProtocol]] = {
    "psycopg2": PsycoPG2Adapter
}


def _make_driver_adapter(
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


class Patterns:
    VALID_QUERY = re.compile(r"^\w+$")
    QUERY_NAME_DEFINITION = re.compile(r"--\s*name\s*:\s*")


class QueryLoader:
    def __init__(
        self, driver_adapter: DriverAdapterProtocol, record_classes: Optional[Dict]
    ):
        self.driver_adapter = driver_adapter
        self.record_classes = record_classes if record_classes is not None else {}

    def _make_query_datum(self, query_str: str):
        lines = [line.strip() for line in query_str.strip().splitlines()]
        query_name = lines[0].replace("-", "_")
        operation_type = SQLOperationType.SELECT

        sql = "\n".join(lines[1:])

        if not Patterns.VALID_QUERY.match(query_name) or not query_name:
            raise SQLParseException(
                f'name must convert to valid python variable, got "{query_name}".'
            )

        sql = self.driver_adapter.process_sql(query_name, operation_type, sql.strip())

        return QueryDatum(query_name, operation_type, sql, record_class=None)

    def load_query_data_from_sql(self, sql: str) -> List[QueryDatum]:
        query_data = []
        query_sql_strs = Patterns.QUERY_NAME_DEFINITION.split(sql)

        for query_sql_str in query_sql_strs[1:]:
            query_data.append(self._make_query_datum(query_sql_str))
        return query_data

    def load_query_data_from_file(self, file_path: Path) -> List[QueryDatum]:
        with file_path.open() as fp:
            return self.load_query_data_from_sql(fp.read())

    def load_query_data_from_dir(self, dir_path: Path) -> QueryDataTree:
        if not dir_path.is_dir():
            raise ValueError(f"The path {dir_path} must be a directory")

        def _recurse_load_query_data_tree(path):
            # queries = Queries()
            query_data_tree = {}
            for p in path.iterdir():
                if p.is_file() and p.suffix != ".sql":
                    continue
                elif p.is_file() and p.suffix == ".sql":
                    for query_datum in self.load_query_data_from_file(p):
                        query_data_tree[query_datum.query_name] = query_datum
                elif p.is_dir():
                    child_name = p.relative_to(dir_path).name
                    child_query_data_tree = _recurse_load_query_data_tree(p)
                    query_data_tree[child_name] = child_query_data_tree
                else:
                    # This should be practically unreachable.
                    raise SQLLoadException(
                        f"The path must be a directory or file, got {p}"
                    )
            return query_data_tree

        return _recurse_load_query_data_tree(dir_path)


def load_from_str(
    sql: str,
    driver_adapter: Union[str, Callable[..., DriverAdapterProtocol]],
    loader_cls: Type[QueryLoader] = QueryLoader,
    queries_cls: Type[QueriesContainer] = QueriesContainer,
):
    adapter = _make_driver_adapter(driver_adapter)
    query_loader = loader_cls(adapter, record_classes=None)
    query_data = query_loader.load_query_data_from_sql(sql)
    return queries_cls(adapter).load_from_list(query_data)


def load_from_file(
    sql_path: Union[str, Path],
    driver_adapter: Union[str, Callable[..., DriverAdapterProtocol]],
    loader_cls: Type[QueryLoader] = QueryLoader,
    queries_cls: Type[QueriesContainer] = QueriesContainer,
):
    path = Path(sql_path)

    if not path.exists():
        raise SQLLoadException(f"File does not exist: {path}")

    adapter = _make_driver_adapter(driver_adapter)
    query_loader = loader_cls(adapter, record_classes=None)

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
    loader_cls: Type[QueryLoader] = QueryLoader,
    queries_cls: Type[QueriesContainer] = QueriesContainer,
):
    return load_from_file(
        sql_path=Path.cwd(),
        driver_adapter=driver_adapter,
        loader_cls=loader_cls,
        queries_cls=queries_cls,
    )
