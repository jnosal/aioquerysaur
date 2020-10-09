from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Union,
)

from typing_extensions import Protocol


class SQLOperationType(Enum):
    SELECT = 1


class QueryDatum(NamedTuple):
    query_name: str
    operation_type: SQLOperationType
    sql: str
    record_class: Any = None


class QueryFn(Protocol):
    __name__: str
    sql: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...


QueryDataTree = Dict[str, Union[QueryDatum, Dict]]


class SyncDriverAdapterProtocol(Protocol):
    def process_sql(self, query_name: str, op_type: SQLOperationType, sql: str) -> str:
        ...

    def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Union[List, Dict],
        record_class: Optional[Callable],
    ) -> List:
        ...


class AsyncDriverAdapterProtocol(Protocol):
    def process_sql(self, query_name: str, op_type: SQLOperationType, sql: str) -> str:
        ...

    async def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: Union[List, Dict],
        record_class: Optional[Callable],
    ) -> List:
        ...


DriverAdapterProtocol = Union[SyncDriverAdapterProtocol, AsyncDriverAdapterProtocol]


class FileQueryLoaderProtocol(Protocol):
    def __init__(
        self, driver_adapter: DriverAdapterProtocol, record_classes: Optional[Dict]
    ):
        ...

    def load_query_data_from_file(self, file_path: Path) -> List[QueryDatum]:
        ...

    def load_query_data_from_dir(self, dir_path: Path) -> QueryDataTree:
        ...


class TextQueryLoaderProtocol(FileQueryLoaderProtocol):
    def load_query_data_from_sql(self, sql: str) -> List[QueryDatum]:
        ...
