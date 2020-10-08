import re

_VAR_PATTERN = re.compile(
    r'(?P<dblquote>"[^"]+")|'
    r"(?P<quote>\'[^\']+\')|"
    r"(?P<lead>[^:]):(?P<var_name>[\w-]+)(?P<trail>[^:]?)"
)


def replacer(match):
    gd = match.groupdict()
    if gd["dblquote"] is not None:
        return gd["dblquote"]
    elif gd["quote"] is not None:
        return gd["quote"]
    else:
        return f'{gd["lead"]}%({gd["var_name"]})s{gd["trail"]}'


class PsycoPG2Adapter:

    @staticmethod
    def process_sql(_query_name, _op_type, sql):
        return _VAR_PATTERN.sub(replacer, sql)

    @staticmethod
    def select(conn, _query_name, sql, parameters, record_class=None):
        with conn.cursor() as cur:
            cur.execute(sql, parameters)
            results = cur.fetchall()
            if record_class is not None:
                column_names = [c.name for c in cur.description]
                results = [record_class(**dict(zip(column_names, row))) for row in results]
        return results
