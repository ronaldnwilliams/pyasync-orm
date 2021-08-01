from typing import List, Optional, Tuple


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

    def add(
        self,
        where_dict: dict,
        not_: bool = False,
    ):
        self.conditions_strings.append(
            'NOT ' if not_ else ''
            + '('
            + 'AND, '.join([
                str(Condition(key=key, value=value))
                for key, value in where_dict.items()
            ])
            + ')'
        )

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

    def _extract_values(self, values_dict: dict) -> dict:
        new_values = tuple(values_dict.values())
        placeholder_values = list(range(len(self.values) + 1, len(new_values) + 1))
        self.values += new_values
        return {
            key: placeholder_values.pop(0)
            for key in values_dict
        }

    def add_where(
        self,
        where_dict: dict,
        not_: bool = False,
    ):
        extracted_where_dict = self._extract_values(values_dict=where_dict)
        self.where.add(where_dict=extracted_where_dict, not_=not_)

    def build_select(self) -> Tuple[str, Tuple]:
        return (
            f'SELECT * FROM {self.table_name} {self.where}',
            self.values,
        )

    def build_update(self, set_dict: dict) -> Tuple[str, Tuple]:
        extracted_set_dict = self._extract_values(values_dict=set_dict)
        set_values = ', '.join(
            f'{key} = {value}'
            for key, value in extracted_set_dict
        )
        return (
            f'UPDATE {self.table_name} SET {set_values} {self.where} RETURNING *',
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

    @classmethod
    def build_insert(
        cls,
        table_name: str,
        column_list: List[str],
    ) -> str:
        if not column_list:
            columns, values = 'DEFAULT', 'VALUES'
        else:
            columns = f'({", ".join(column_list)})' if column_list else ''
            values = f'VALUES({", ".join([f"${num}" for num in range(1, len(column_list) + 1)])})' if columns else ''
        return f'INSERT INTO {table_name} {columns} {values} RETURNING *'
