from typing import TYPE_CHECKING, Type, Optional, List

from pyasync_orm.database import Database
from pyasync_orm.sql import SQL

if TYPE_CHECKING:
    from pyasync_orm.models import Model


class ORM:
    database = Database()

    def __init__(
        self,
        model_class: Type['Model'],
        sql: Optional[SQL] = None,
    ):
        self._model_class = model_class
        self._sql = sql

    def _get_orm(self) -> 'ORM':
        return ORM(
            self._model_class,
            sql=SQL(self._model_class.table_name),
        ) if self._sql is None else self

    async def create(self, **kwargs) -> 'Model':
        sql = SQL.build_insert(
            table_name=self._model_class.table_name,
            column_list=list(kwargs.keys()),
        )
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *kwargs.values())
        return self._model_class.from_db(results[0])

    def filter(self, **kwargs) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_where(where_dict=kwargs)
        return orm

    def exclude(self, **kwargs) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_where(where_dict=kwargs, not_=True)
        return orm

    async def get(self, **kwargs) -> 'Model':
        orm = self._get_orm()
        orm._sql.add_where(where_dict=kwargs)
        sql, values = orm._sql.build_select()
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        if len(results) > 1:
            raise ValueError(f'{self._model_class.__name__} get query found more than one record.')
        return self._model_class.from_db(results[0])

    async def all(self) -> List['Model']:
        orm = self._get_orm()
        sql, values = orm._sql.build_select()
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]

    async def update(self, **kwargs) -> List['Model']:
        orm = self._get_orm()
        sql, values = orm._sql.build_update(set_dict=kwargs)
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]

    async def delete(self) -> List['Model']:
        orm = self._get_orm()
        sql, values = orm._sql.build_delete()
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]

    async def count(self):
        orm = self._get_orm()
        sql, values = orm._sql.build_count()
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return results[0]['count']
