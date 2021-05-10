from typing import Optional, TYPE_CHECKING, Type

from pyasync_orm.database import Database
from pyasync_orm.sql.sql import SQL


if TYPE_CHECKING:
    from pyasync_orm.models import Model


class ORM:
    database = Database()

    def __init__(self, model_class: Type['Model']):
        self.model_class = model_class
        self._sql: Optional[SQL] = None
        self._values = ()

    def _get_orm(self) -> 'ORM':
        if self._sql is None:
            orm = ORM(self.model_class)
            orm._sql = SQL(self.model_class.meta.table_name)
        else:
            orm = self
        return orm

    def _get_sql(self) -> SQL:
        return self._sql or SQL(self.model_class.meta.table_name)

    def filter(self, **kwargs) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_where(where_list=list(kwargs.keys()))
        orm._values += tuple(kwargs.values())
        return orm

    def exclude(self, **kwargs) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_where(where_list=list(kwargs.keys()), exclude=True)
        orm._values += tuple(kwargs.values())
        return orm

    def order_by(self, *args: str) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_order_by(args)
        return orm

    def limit(self, number: int) -> 'ORM':
        orm = self._get_orm()
        orm._sql.set_limit(number)
        return orm

    async def count(self):
        """
        Counting rows in big tables (millions of rows) can be slow.
        Possibly add an estimate method but requires some tinkering with Analyze and Vacuum.
        """
        sql_string = self._get_sql().create_select_sql_string(columns='COUNT(*)')
        async with self.database.pool.acquire() as connection:
            results = await connection.fetch(sql_string, *self._values)
        return results[0]['count']

    def convert_to_model(self, record_dict: dict):
        model = self.model_class()
        model.__dict__.update(record_dict)
        return model

    async def create(self, **kwargs):
        values = self._values + tuple(kwargs.values())
        sql_string = self._get_sql().create_insert_sql_string(columns=list(kwargs.keys()))
        async with self.database.pool.acquire() as connection:
            async with connection.transaction():
                results = await connection.fetch(sql_string, *values)
        return self.convert_to_model(dict(results[0]))

    async def bulk_create(self, **kwargs):
        pass

    async def get(self, **kwargs):
        values = self._values + tuple(kwargs.values())
        sql = self._get_sql()
        sql.add_where(list(kwargs.keys()))
        sql_string = sql.create_select_sql_string(columns='*')
        async with self.database.pool.acquire() as connection:
            results = await connection.fetch(sql_string, *values)
        return self.convert_to_model(dict(results[0]))

    async def all(self):
        sql_string = self._get_sql().create_select_sql_string(columns='*')
        async with self.database.pool.acquire() as connection:
            results = await connection.fetch(sql_string, *self._values)
        return [self.convert_to_model(dict(result)) for result in results]

    async def update(self, **kwargs):
        values = self._values + tuple(kwargs.values())
        sql_string = self._get_sql().create_update_sql_string(set_columns=list(kwargs.keys()))
        async with self.database.pool.acquire() as connection:
            async with connection.transaction():
                results = await connection.fetch(sql_string, *values)
        return [self.convert_to_model(dict(result)) for result in results]

    async def bulk_update(self, **kwargs):
        pass

    async def delete(self):
        sql_string = self._get_sql().create_delete_sql_string()
        async with self.database.pool.acquire() as connection:
            async with connection.transaction():
                results = await connection.fetch(sql_string, *self._values)
        return [self.convert_to_model(dict(result)) for result in results]
