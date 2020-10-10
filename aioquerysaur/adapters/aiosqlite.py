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

    @staticmethod
    async def select_one(conn, _query_name, sql, parameters, record_class=None):
        async with conn.execute(sql, parameters) as cur:
            result = await cur.fetchone()
            if result is not None and record_class is not None:
                column_names = [c[0] for c in cur.description]
                result = record_class(**dict(zip(column_names, result)))
        return result

    @staticmethod
    async def insert_update_delete(conn, _query_name, sql, parameters):
        cur = await conn.execute(sql, parameters)
        await cur.close()

    @staticmethod
    async def insert_update_delete_many(conn, _query_name, sql, parameters):
        cur = await conn.executemany(sql, parameters)
        await cur.close()
