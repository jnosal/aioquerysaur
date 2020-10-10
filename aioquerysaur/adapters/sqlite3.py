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

    @staticmethod
    def select_one(conn, _query_name, sql, parameters, record_class=None):
        cur = conn.cursor()
        cur.execute(sql, parameters)
        result = cur.fetchone()
        if result is not None and record_class is not None:
            column_names = [c[0] for c in cur.description]
            result = record_class(**dict(zip(column_names, result)))
        cur.close()
        return result

    @staticmethod
    def insert_update_delete(conn, _query_name, sql, parameters):
        conn.execute(sql, parameters)

    @staticmethod
    def insert_update_delete_many(conn, _query_name, sql, parameters):
        conn.execute(sql, parameters)
