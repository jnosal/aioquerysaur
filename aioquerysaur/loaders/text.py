import re

from ..exceptions import SQLLoadException, SQLParseException
from ..models import QueryDatum, SQLOperationType


class Patterns:
    VALID_QUERY = re.compile(r"^\w+$")
    QUERY_NAME_DEFINITION = re.compile(r"--\s*name\s*:\s*")


class DescriptionSuffix:
    SELECT_ONE = "$"
    INSERT_UPDATE_DELETE = ">"
    INSERT_UPDATE_DELETE_MANY = ">>"


class TextLoader:
    def __init__(self, driver_adapter, record_classes):
        self.driver_adapter = driver_adapter
        self.record_classes = record_classes if record_classes is not None else {}

    def _make_query_datum(self, query_str):
        lines = [line.strip() for line in query_str.strip().splitlines()]
        query_name = lines[0].replace("-", "_")

        if query_name.endswith(DescriptionSuffix.SELECT_ONE):
            operation_type = SQLOperationType.SELECT_ONE
            query_name = query_name[:-1]
        elif query_name.endswith(DescriptionSuffix.INSERT_UPDATE_DELETE_MANY):
            operation_type = SQLOperationType.INSERT_UPDATE_DELETE_MANY
            query_name = query_name[:-2]
        elif query_name.endswith(DescriptionSuffix.INSERT_UPDATE_DELETE):
            operation_type = SQLOperationType.INSERT_UPDATE_DELETE
            query_name = query_name[:-1]
        else:
            operation_type = SQLOperationType.SELECT

        sql = "\n".join(lines[1:])

        if not Patterns.VALID_QUERY.match(query_name) or not query_name:
            raise SQLParseException(
                f'name must convert to valid python variable, got "{query_name}".'
            )

        sql = self.driver_adapter.process_sql(query_name, operation_type, sql.strip())

        return QueryDatum(query_name, operation_type, sql, record_class=None)

    def load_query_data_from_sql(self, sql):
        query_data = []
        query_sql_strs = Patterns.QUERY_NAME_DEFINITION.split(sql)

        for query_sql_str in query_sql_strs[1:]:
            query_data.append(self._make_query_datum(query_sql_str))
        return query_data

    def load_query_data_from_file(self, file_path):
        with file_path.open() as fp:
            return self.load_query_data_from_sql(fp.read())

    def load_query_data_from_dir(self, dir_path):
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
