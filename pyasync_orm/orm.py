from typing import TYPE_CHECKING, Type, Optional, List, TypeVar, Tuple

from pyasync_orm.database import Database
from pyasync_orm.sql import SQL

if TYPE_CHECKING:
    from pyasync_orm.models import Model
    from pyasync_orm.fields import SearchCondition

    # removes IDE warning on subclasses
    ModelType = TypeVar('ModelType', bound=Model)


class ORM:
    database = Database()

    def __init__(
        self,
        model_class: Type['ModelType'],
        sql: Optional[SQL] = None,
    ):
        self._model_class = model_class
        self._sql = sql

    def _get_orm(self) -> 'ORM':
        return ORM(
            self._model_class,
            sql=SQL(self._model_class.table_name),
        ) if self._sql is None else self

    def _add_search_conditions(
        self,
        search_conditions: Tuple['SearchCondition'],
    ):
        for search_condition in search_conditions:
            self._sql.add_where(
                field_name=search_condition.field_name,
                symbol=search_condition.symbol,
                field_value=search_condition.field_value,
            )

    async def create(self, model: Optional['ModelType'] = None) -> 'ModelType':
        orm = self._get_orm()
        fields_dict = model.orm_fields if model else {}
        sql, values = orm._sql.build_insert(fields_dict=fields_dict)
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return self._model_class.from_db(results[0])

    def filter(self, *search_conditions: 'SearchCondition') -> 'ORM':
        orm = self._get_orm()
        orm._add_search_conditions(search_conditions=search_conditions)
        return orm

    # TODO do I still need exclude or can I use != instead
    # def exclude(self, **kwargs) -> 'ORM':
    #     orm = self._get_orm()
    #     orm._sql.add_where(where_dict=kwargs, not_=True)
    #     return orm

    async def get(self, *search_conditions: 'SearchCondition') -> 'ModelType':
        orm = self._get_orm()
        orm._add_search_conditions(search_conditions=search_conditions)
        sql, values = orm._sql.build_select()
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        if len(results) > 1:
            raise ValueError(
                f'{self._model_class.__name__} get query '
                'found more than one record.'
            )
        return self._model_class.from_db(results[0])

    async def all(self) -> List['ModelType']:
        orm = self._get_orm()
        sql, values = orm._sql.build_select()
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]

    async def update(self, model: 'ModelType') -> List['ModelType']:
        orm = self._get_orm()
        sql, values = orm._sql.build_update(fields_dict=model.orm_fields)
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]

    async def delete(self) -> List['ModelType']:
        orm = self._get_orm()
        sql, values = orm._sql.build_delete()
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return [self._model_class.from_db(result) for result in results]

    async def count(self) -> int:
        orm = self._get_orm()
        sql, values = orm._sql.build_count()
        async with self.database.get_connection() as connection:
            results = await connection.fetch(sql, *values)
        return results[0]['count']
