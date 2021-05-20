from typing import Optional, TYPE_CHECKING, Type, List, Tuple

import inflection

from pyasync_orm.database import Database
from pyasync_orm.sql.sql import SQL, OPERATOR_LOOKUPS, OPERATOR_LOOKUPS_KEY_SET

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

    def _add_inner_joins(self, field: str):
        sql = self._get_sql()
        split_fields = field.split('__')
        current_model = self.model_class
        for split_field in split_fields:
            join_model_field = current_model.meta.table_fields.get(split_field)
            if hasattr(join_model_field, 'model'):
                join_model = join_model_field.model
                sql.add_inner_joins((
                    join_model.meta.table_name,
                    f'({current_model.meta.table_name}.{split_field}_id = {join_model.meta.table_name}.id)',
                ))
                current_model = join_model

    def _add_table_names(self, fields: List[str]) -> Tuple[List[str], List[str]]:
        model_field_list = []
        related_field_list = []
        table_fields = self.model_class.meta.table_fields
        for field in fields:
            if field in table_fields:
                if getattr(table_fields[field], 'is_db_column', True):
                    model_field_list.append(f'{self.model_class.meta.table_name}.{field}')
                else:
                    if not field.endswith('_set') and f'{field}_id' not in model_field_list:
                        model_field_list.append(f'{self.model_class.meta.table_name}.{field}_id')
            else:
                self._add_inner_joins(field)
                split_fields = field.split('__')
                if split_fields[-1] in OPERATOR_LOOKUPS_KEY_SET:
                    if len(split_fields) == 2:
                        model_field_list.append(f'{self.model_class.meta.table_name}.{split_fields[0]}__{split_fields[1]}')
                    else:
                        split_fields = split_fields[-3:]
                        related_field_list.append(f'{inflection.tableize(split_fields[0])}.{split_fields[1]}__{split_fields[2]}')
                else:
                    split_fields = split_fields[-2:]
                    related_field_list.append(f'{inflection.tableize(split_fields[0])}.{split_fields[1]}')
        return model_field_list, related_field_list

    def _get_select_columns_from_related_tables(
            self,
            related_names: Tuple[str],
    ) -> List[str]:
        select_columns = []
        for related_name in related_names:
            self._add_inner_joins(related_name)
            split_related_names = related_name.split('__')
            related_model_meta = self.model_class.meta
            select_columns += [
                f'{related_model_meta.table_name}.{field_name}'
                for field_name, field_value in related_model_meta.table_fields.items()
                if getattr(field_value, 'is_db_column', True)
            ]
            if len(split_related_names) > 1:
                for split_related_name in split_related_names:
                    related_model_meta = related_model_meta.table_fields[split_related_name].model.meta
                    select_columns += [
                        f'{related_model_meta.table_name}.{field_name}'
                        for field_name, field_value in related_model_meta.table_fields.items()
                        if getattr(field_value, 'is_db_column', True)
                    ]
        return select_columns

    def filter(self, **kwargs) -> 'ORM':
        orm = self._get_orm()
        # TODO left off here with order__customer__age__gt=18 failing
        model_field_list, related_field_list = orm._add_table_names(list(kwargs.keys()))
        orm._sql.add_where(where_list=model_field_list + related_field_list)
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
        sql = self._get_sql()
        sql.add_select_columns(['COUNT(*)'])
        sql_string = sql.create_select_sql_string()
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
            fields = list(self.model_class.meta.table_fields.keys())
            model_columns, related_columns = self._add_table_names(fields)
            sql.add_select_columns(model_columns)
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
