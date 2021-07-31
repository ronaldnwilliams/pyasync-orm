from typing import TYPE_CHECKING, Type, Optional

from pyasync_orm.database import Database
from pyasync_orm.sql import SQL

if TYPE_CHECKING:
    from pyasync_orm.models import Model


class ORM:
    database = Database()

    def __init__(self, model_class: Type['Model'], sql: Optional[SQL] = None):
        self._model_class = model_class
        self._sql = sql

    def _get_orm(self):
        return ORM(self._model_class, sql=SQL()) if self._sql is None else self

    async def create(self, **kwargs):
        sql = SQL.build_insert(
            table_name=self._model_class.table_name,
            column_list=list(kwargs.keys()),
        )
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *kwargs.values())
        return self._model_class.from_db(results[0])

    def filter(self, **kwargs):
        orm = self._get_orm()
        orm._sql.add_where(kwargs)
        return orm

    def exclude(self, **kwargs):
        orm = self._get_orm()
        orm._sql.add_where_not(kwargs)
        return orm

    async def get(self, **kwargs):
        orm = self._get_orm()
        orm._sql.add_where(kwargs)
        sql, values = orm._sql.build_select(table_name=self._model_class.table_name)
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        if len(results) > 1:
            raise ValueError(f'{self._model_class.__name__} get query found more than one record.')
        return self._model_class.from_db(results[0])

    async def all(self):
        orm = self._get_orm()
        sql, values = orm._sql.build_select(table_name=self._model_class.table_name)
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]

    async def update(self):
        orm = self._get_orm()
        sql, values = orm._sql.build_update(table_name=self._model_class.table_name)
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]

    async def delete(self):
        orm = self._get_orm()
        sql, values = orm._sql.build_delete(table_name=self._model_class.table_name)
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]
