class SQLite3DriverAdapter:
    @staticmethod
    def process_sql(_query_name, _op_type, sql):
        return sql

    @staticmethod
    def select(conn, _query_name, sql, parameters, record_class=None):
        cur = conn.cursor()
        cur.execute(sql, parameters)
        results = cur.fetchall()
        if record_class is not None:
            column_names = [c[0] for c in cur.description]
            results = [record_class(**dict(zip(column_names, row))) for row in results]
        cur.close()
        return results
