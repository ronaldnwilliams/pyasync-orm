from typing import Tuple, List

from pyasync_orm.sql.commands import InsertSQL, SelectSQL, UpdateSQL, DeleteSQL


LOOKUPS = {
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'in': 'IN',
    'isnull': 'ISNULL',
}


def pop_operator(key: str) -> Tuple[List[str], str]:
    key_parts = key.split('__')
    last_key_part = key_parts[len(key_parts) - 1]
    operator = LOOKUPS.get(last_key_part)

    if operator:
        key_parts.remove(last_key_part)

    return key_parts, operator or '='


def create_where_string(where: dict, exclude: bool = False) -> str:
    where_string = []
    for key, value in where.items():
        key_parts, operator = pop_operator(key)
        # TODO handle table relationships here
        where_string.append(f'{key_parts[0]} {operator} {value}')
    joined_wheres = ' AND '.join(where_string)
    return f'{"NOT (" if exclude else ""}{joined_wheres}{")" if exclude else ""}'


class SQL:
    def __init__(self, table_name):
        self.table_name = table_name
        self.where = []
        self.order_by = []
        self.limit = None

    def add_where(self, where: dict, exclude: bool = False):
        where = create_where_string(where, exclude=exclude)
        self.where.append(where)

    def add_order_by(self, order_by_args: Tuple[str]):
        self.order_by += list(order_by_args)

    def set_limit(self, number: int):
        self.limit = number

    def insert(self):
        return InsertSQL(table_name=self.table_name)

    def select(self):
        return SelectSQL(table_name=self.table_name)

    def update(self):
        return UpdateSQL(table_name=self.table_name)

    def delete(self):
        return DeleteSQL(table_name=self.table_name)
