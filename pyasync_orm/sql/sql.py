from typing import Tuple, List, Union

from pyasync_orm.sql.commands import InsertSQL, SelectSQL, UpdateSQL, DeleteSQL


OPERATOR_LOOKUPS = {
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'in': 'IN',
    'isnull': 'ISNULL',
}
OPERATOR_LOOKUPS_KEY_SET = set(OPERATOR_LOOKUPS.keys())


def pop_operator(key: str) -> Tuple[List[str], str]:
    key_parts = key.split('__')
    last_key_part = key_parts[len(key_parts) - 1]
    operator = OPERATOR_LOOKUPS.get(last_key_part)

    if operator:
        key_parts.remove(last_key_part)

    return key_parts, operator or '='


def create_where_string(
        where_list: List[str],
        starting_value: int,
        exclude: bool = False,
) -> str:
    where_string = []
    value = starting_value
    for key in where_list:
        key_parts, operator = pop_operator(key)
        # TODO maybe handle table relationships here
        where_string.append(f'{key_parts[0]} {operator} ${value}')
        value += 1
    joined_wheres = ' AND '.join(where_string)
    return f'{"NOT " if exclude else ""}({joined_wheres})'


def create_set_columns_string(
        set_columns_list: List[str],
        starting_value: int,
) -> str:
    set_columns_strings = []
    value = starting_value
    for key in set_columns_list:
        set_columns_strings.append(f'{key} = ${value}')
        value += 1
    return ', '.join(set_columns_strings)


class SQL:
    def __init__(self, table_name):
        self.table_name = table_name
        self.inner_joins = None
        self.value_count = 0
        self.select_columns = ''
        self.where = []
        self.order_by = []
        self.limit = None
        self.set_columns_string = ''
        self.returning = '*'

    def add_select_columns(self, columns: List[str]):
        if self.select_columns == '':
            columns = [f'{column} AS {column.replace(".", "_pyaormsep_")}' for column in columns]
            self.select_columns = ', '.join(columns)

    def add_returning(self, returning: List[str]):
        returning = [f'{column} AS {column.replace(".", "_pyaormsep_")}' for column in returning]
        self.returning = ', '.join(returning)

    def add_aggregate(self, aggregate: str):
        self.select_columns = aggregate

    def add_where(
            self,
            where_list: List[str],
            exclude: bool = False,
    ):
        where_string = create_where_string(
            where_list=where_list,
            starting_value=self.value_count + 1,
            exclude=exclude,
        )
        self.value_count += len(where_list)
        self.where.append(where_string)

    def add_inner_joins(self, inner_joins: Tuple[str, str]):
        if self.inner_joins is None:
            self.inner_joins = ()
        if inner_joins not in self.inner_joins:
            self.inner_joins += (inner_joins, )

    def set_set_columns(
            self,
            set_columns: List[str],
    ):
        set_columns_string = create_set_columns_string(
            set_columns_list=set_columns,
            starting_value=self.value_count + 1,
        )
        self.value_count += len(set_columns)
        self.set_columns_string = set_columns_string

    def add_order_by(self, order_by_args: Tuple[str]):
        self.order_by += list(order_by_args)

    def set_limit(self, number: int):
        self.limit = number

    def create_insert_sql_string(self, columns: List[str]):
        return InsertSQL(
            table_name=self.table_name,
            columns=columns,
            # TODO dynamic returning
            returning=self.returning,
        ).sql_string

    def create_select_sql_string(self):
        return SelectSQL(
            table_name=self.table_name,
            columns=self.select_columns,
            inner_joins=self.inner_joins,
            where=self.where,
            order_by=self.order_by,
            limit=self.limit,
        ).sql_string

    def create_update_sql_string(self, set_columns: List[str]):
        self.set_set_columns(set_columns)
        return UpdateSQL(
            table_name=self.table_name,
            set_columns_string=self.set_columns_string,
            where=self.where,
            # TODO dynamic returning
            returning=self.returning,
        ).sql_string

    def create_delete_sql_string(self):
        return DeleteSQL(
            table_name=self.table_name,
            where=self.where,
            # TODO dynamic returning
            returning=self.returning,
        ).sql_string
