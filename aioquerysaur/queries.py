from types import MethodType
from typing import Any, Callable, List, Tuple, Set, cast

from .models import DriverAdapterProtocol, QueryDatum, QueryDataTree, QueryFn, SQLOperationType


def _params(args, kwargs):
    if len(kwargs) > 0:
        return kwargs
    else:
        return args


def _query_fn(fn: Callable[..., Any], name: str, sql: str) -> QueryFn:
    qfn = cast(QueryFn, fn)
    qfn.__name__ = name
    qfn.sql = sql
    return qfn


def _make_sync_fn(query_datum: QueryDatum) -> QueryFn:
    query_name, operation_type, sql, record_class = query_datum

    if operation_type == SQLOperationType.SELECT:
        def fn(self: QueriesContainer, conn, *args, **kwargs):
            return self.driver_adapter.select(
                conn, query_name, sql, _params(args, kwargs), record_class
            )
    else:
        raise ValueError(f"Unknown operation_type: {operation_type}")

    return _query_fn(fn, query_name, sql)


def _make_async_fn(fn: QueryFn) -> QueryFn:
    async def afn(self: QueriesContainer, conn, *args, **kwargs):
        return await fn(self, conn, *args, **kwargs)

    return _query_fn(afn, fn.__name__, fn.__doc__,)


def _make_ctx_mgr(fn: QueryFn) -> QueryFn:
    def ctx_mgr(self, conn, *args, **kwargs):
        return self.driver_adapter.select_cursor(conn, fn.__name__, fn.sql, _params(args, kwargs))

    return _query_fn(ctx_mgr, f"{fn.__name__}_cursor", fn.__doc__)


def _create_methods(query_datum: QueryDatum, is_aio: bool) -> List[Tuple[str, QueryFn]]:
    fn = _make_sync_fn(query_datum)
    if is_aio:
        fn = _make_async_fn(fn)

    ctx_mgr = _make_ctx_mgr(fn)

    if query_datum.operation_type == SQLOperationType.SELECT:
        return [(fn.__name__, fn), (ctx_mgr.__name__, ctx_mgr)]
    else:
        return [(fn.__name__, fn)]


class QueriesContainer:
    """
        Dynamic methods build from SQL queries
    """

    def __init__(self, driver_adapter: DriverAdapterProtocol):
        self.driver_adapter: DriverAdapterProtocol = driver_adapter
        self.is_aio: bool = getattr(driver_adapter, "is_aio_driver", False)
        self._available_queries: Set[str] = set()

    @property
    def available(self) -> List[str]:
        return sorted(self._available_queries)

    def __repr__(self):
        return "Queries(" + self.available.__repr__() + ")"

    def add_query(self, query_name: str, fn: Callable):
        setattr(self, query_name, fn)
        self._available_queries.add(query_name)

    def add_queries(self, queries: List[Tuple[str, QueryFn]]):
        for query_name, fn in queries:
            self.add_query(query_name, MethodType(fn, self))

    def add_child_queries(self, child_name: str, child_queries: "QueriesContainer"):
        setattr(self, child_name, child_queries)
        for child_query_name in child_queries.available:
            self._available_queries.add(f"{child_name}.{child_query_name}")

    def load_from_list(self, query_data: List[QueryDatum]):
        for query_datum in query_data:
            self.add_queries(_create_methods(query_datum, self.is_aio))
        return self

    def load_from_tree(self, query_data_tree: QueryDataTree):
        for key, value in query_data_tree.items():
            if isinstance(value, dict):
                self.add_child_queries(key, QueriesContainer(self.driver_adapter).load_from_tree(value))
            else:
                self.add_queries(_create_methods(value, self.is_aio))
        return self
