from typing import Optional, TYPE_CHECKING, Type, List, Tuple

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

    def _add_table_names(self, fields: List[str]) -> List[str]:
        field_list = []
        table_fields = self.model_class.meta.table_fields
        for field in fields:
            if field in table_fields:
                if 'REFERENCES' in table_fields[field].get_sql_string() and not field.endswith('_id'):
                    field = f'{field}_id'
                field_list.append(f'{self.model_class.meta.table_name}.{field}')
        return field_list

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

    def _get_select_columns_from_related_tables(
            self,
            related_names: Tuple[str],
    ) -> List[str]:
        select_columns = []
        for related_name in related_names:
            if '__' not in related_name:
                related_model_meta = self.model_class.meta.table_fields[related_name].model.meta
                select_columns += [
                    f'{related_model_meta.table_name}.{field_name}'
                    for field_name in related_model_meta.table_fields.keys()
                ]
            else:
                # TODO left off here recursion?
                x = related_name.split('__')
        return select_columns

    def select_related(self, *args: str):
        orm = self._get_orm()
        select_columns = self._get_select_columns_from_related_tables(args)
        orm._sql.add_select_columns(select_columns)
        return orm

    def prefect_related(self, *args: str):
        pass

    async def count(self):
        """
        Counting rows in big tables (millions of rows) can be slow.
        Possibly add an estimate method but requires some tinkering with Analyze and Vacuum.
        """
        sql_string = self._get_sql().create_select_sql_string(columns='COUNT(*)')
        async with self.database.pool.acquire() as connection:
            results = await connection.fetch(sql_string, *self._values)
        return results[0]['count']

    def convert_to_model(self, record: dict):
        model = self.model_class()
        model.__dict__.update(record)
        # model.update_relationships(record)
        return model

    async def create(self, **kwargs):
        model_instances = []
        for kwargs_ in list(kwargs.items()):
            key, value = kwargs_
            if getattr(value, '__class__', object).__base__ == self.model_class.__base__:
                model_instances.append(value)
                del kwargs[key]
        columns = list(kwargs.keys()) + [f'{instance.__class__.__name__.lower()}_id' for instance in model_instances]
        values = self._values + tuple(kwargs.values()) + tuple(instance.id for instance in model_instances)
        # TODO can we return the related model too?
        sql_string = self._get_sql().create_insert_sql_string(columns=columns)
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
