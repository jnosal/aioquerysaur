class AioSQLiteAdapter:
    is_aio_driver = True

    @staticmethod
    def process_sql(_query_name, _op_type, sql):
        return sql

    @staticmethod
    async def select(conn, _query_name, sql, parameters, record_class=None):
        async with conn.execute(sql, parameters) as cur:
            results = await cur.fetchall()
            if record_class is not None:
                column_names = [c[0] for c in cur.description]
                results = [
                    record_class(**dict(zip(column_names, row))) for row in results
                ]
        return results
