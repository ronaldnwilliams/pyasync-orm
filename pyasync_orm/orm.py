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

    def _get_inner_joins(self, related_columns: Tuple[str]):
        pass

    def _get_orm(self) -> 'ORM':
        if self._sql is None:
            orm = ORM(self.model_class)
            orm._sql = SQL(self.model_class.meta.table_name)
        else:
            orm = self
        return orm

    def _get_sql(self) -> SQL:
        return self._sql or SQL(self.model_class.meta.table_name)

    def _add_inner_joins(self, field: str):
        sql = self._get_sql()
        # order__customer__name order__price order__customer__wallet__primary
        split_fields = field.split('__')
        current_model = self.model_class
        for split_field in split_fields:
            join_model_field = current_model.meta.table_fields[split_field]
            if hasattr(join_model_field, 'model'):
                join_model = join_model_field.model
                sql.add_inner_join((
                    join_model.meta.table_name,
                    f'({current_model}.{split_field}_id = {join_model.meta.table_name}.id)',
                ))
                current_model = join_model.meta.table_fields

    def _add_table_names(self, fields: List[str]) -> Tuple[List[str], List[str]]:
        model_field_list = []
        related_field_list = []
        table_fields = self.model_class.meta.table_fields
        for field in fields:
            if field in table_fields:
                if 'REFERENCES' in table_fields[field].get_sql_string() and not field.endswith('_id'):
                    field = f'{field}_id'
                model_field_list.append(f'{self.model_class.meta.table_name}.{field}')
            else:
                self._add_inner_joins(field)
                split_field = field.split('__')[-2:]
                related_field_list.append(f'{split_field[0]}.{split_field[1]}')
        return model_field_list, related_field_list

    def _get_select_columns_from_related_tables(
            self,
            related_names: Tuple[str],
    ) -> List[str]:
        # TODO use _add_inner_joins here
        select_columns = []
        for related_name in related_names:
            split_related_names = related_name.split('__')
            related_model_meta = self.model_class.meta
            select_columns += [
                f'{related_model_meta.table_name}.{field_name}'
                for field_name in related_model_meta.table_fields.keys()
            ]
            if len(split_related_names) > 1:
                for split_related_name in split_related_names:
                    related_model_meta = related_model_meta.table_fields[split_related_name].model.meta
                    select_columns += [
                        f'{related_model_meta.table_name}.{field_name}'
                        for field_name in related_model_meta.table_fields.keys()
                    ]
        return select_columns

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

    def select_related(self, *args: str):
        orm = self._get_orm()
        select_columns = self._get_select_columns_from_related_tables(args)
        orm._sql.add_select_columns(select_columns)
        return orm

    def prefetch_related(self, *args: str):
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
        sql = self._get_sql()
        model_columns, related_columns = self._add_table_names(list(kwargs.keys()))
        sql.add_select_columns(model_columns)
        sql.add_where(model_columns + related_columns)
        sql_string = sql.create_select_sql_string()
        values = self._values + tuple(kwargs.values())
        async with self.database.pool.acquire() as connection:
            results = await connection.fetch(sql_string, *values)
        return self.convert_to_model(dict(results[0]))

    async def all(self):
        sql = self._get_sql()
        if sql.select_columns == '':
            sql.add_select_columns(['*'])
        sql_string = sql.create_select_sql_string()
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
