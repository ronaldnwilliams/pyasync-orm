from typing import List, Optional, Tuple, Any


def not_implemented(*args, **kwargs):
    raise NotImplementedError


LOOKUPS = {
    'exact': not_implemented,
    'iexact': not_implemented,
    'contains': not_implemented,
    'icontains': not_implemented,
    'in': not_implemented,
    'gt': not_implemented,
    'gte': not_implemented,
    'lt': not_implemented,
    'lte': not_implemented,
    'startswith': not_implemented,
    'istartswith': not_implemented,
    'endswith': not_implemented,
    'iendswith': not_implemented,
    'range': not_implemented,
    'date': not_implemented,
    'year': not_implemented,
    'iso_year': not_implemented,
    'month': not_implemented,
    'day': not_implemented,
    'week': not_implemented,
    'week_day': not_implemented,
    'iso_week_day': not_implemented,
    'quarter': not_implemented,
    'time': not_implemented,
    'hour': not_implemented,
    'minute': not_implemented,
    'second': not_implemented,
    'isnull': not_implemented,
    'regex': not_implemented,
    'iregex': not_implemented,
}


class Condition:
    key: str
    operator: str
    value: str

    def __init__(
        self,
        key: str,
        value: str,
    ):
        self.key, self.operator, self.value = self._split_by_operator(
            key=key,
            value=value,
        )

    def __str__(self):
        return f'{self.key} {self.operator} ${self.value}'

    def _split_by_operator(
        self,
        key: str,
        value: str
    ) -> Tuple[str, str, str]:
        operator = '='
        keys_split = key.split('__')
        if len(keys_split) > 1:
            operator_function = LOOKUPS.get(keys_split[-1])
            if operator_function is not None:
                key, operator, value = operator_function(key, value)
        return key, operator, value


class Where:
    conditions_strings: List[str]

    def __init__(self):
        self.conditions_strings = []

    # def add(
    #     self,
    #     where_dict: dict,
    #     not_: bool = False,
    # ):
    #     self.conditions_strings.append(
    #         'NOT ' if not_ else ''
    #         + '('
    #         + 'AND, '.join([
    #             str(Condition(key=key, value=value))
    #             for key, value in where_dict.items()
    #         ])
    #         + ')'
    #     )

    def add(
        self,
        conditional_string: str
    ):
        self.conditions_strings.append(conditional_string)

    def __str__(self):
        if self.conditions_strings:
            where = 'WHERE '
            where += 'AND '.join([condition for condition in self.conditions_strings])
        else:
            where = ''
        return where


class SQL:
    table_name: str
    where: Optional[Where]

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.values = ()
        self.where = Where()

    # def _extract_values(self, values_dict: dict) -> dict:
    #     new_values = tuple(values_dict.values())
    #     placeholder_values = list(range(len(self.values) + 1, len(new_values) + 1))
    #     self.values += new_values
    #     return {
    #         key: placeholder_values.pop(0)
    #         for key in values_dict
    #     }

    def _swap_value_with_placeholder(self, value: Any) -> str:
        self.values += (value,)
        return f'${len(self.values)}'

    # def add_where(
    #     self,
    #     where_dict: dict,
    #     not_: bool = False,
    # ):
    #     extracted_where_dict = self._extract_values(values_dict=where_dict)
    #     self.where.add(where_dict=extracted_where_dict, not_=not_)

    def add_where(
        self,
        field_name: str,
        symbol: str,
        field_value: Any,
    ):
        placeholder_value = self._swap_value_with_placeholder(
            value=field_value,
        )
        self.where.add(f'{field_name} {symbol} {placeholder_value}')

    def build_select(self) -> Tuple[str, Tuple]:
        return (
            f'SELECT * FROM {self.table_name} {self.where}',
            self.values,
        )

    def build_update(self, fields_dict: dict) -> Tuple[str, Tuple]:
        set_values = ', '.join(
            f'{key} = {self._swap_value_with_placeholder(value)}'
            for key, value in fields_dict.items()
        )
        return (
            f'UPDATE {self.table_name} '
            f'SET {set_values} {self.where} RETURNING *',
            self.values,
        )

    def build_delete(self) -> Tuple[str, Tuple]:
        return (
            f'DELETE FROM {self.table_name} {self.where} RETURNING *',
            self.values,
        )

    def build_count(self) -> Tuple[str, Tuple]:
        return (
            f'SELECT COUNT(*) FROM {self.table_name} {self.where}',
            self.values,
        )

    def build_insert(self, fields_dict: dict) -> Tuple[str, Tuple]:
        if not fields_dict:
            columns, values = 'DEFAULT', ''
        else:
            column_names = ', '.join(fields_dict.keys())
            columns = f'({column_names})'
            placeholders = ', '.join(
                f'{self._swap_value_with_placeholder(value)}'
                for value in fields_dict.values()
            )
            values = f'({placeholders})'
        return (
            f'INSERT INTO {self.table_name} {columns}'
            f' VALUES{values} RETURNING *',
            self.values,
        )
