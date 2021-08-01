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
        pass

    def __str__(self):
        return f'{self.key} {self.operator} {self.value}'


class Where:
    conditions: List[Condition]

    def __init__(
        self,
        kwargs: dict,
        not_: bool = False,
    ):
        pass

    def add(
        self,
        kwargs: dict,
        not_: bool = False,
    ):
        pass

    def __str__(self):
        return 'AND '.join([str(condition) for condition in self.conditions])


class SQL:
    table_name: str
    where: Optional[Where]

    def __init__(self, table_name: str):
        self.table_name = table_name

    def add_where(self, **kwargs):
        pass

    def add_where_not(self, **kwargs):
        pass

    def build_select(self) -> Tuple[str, Tuple[Any]]:
        pass

    def build_update(self) -> Tuple[str, Tuple[Any]]:
        pass

    def build_delete(self) -> Tuple[str, Tuple[Any]]:
        pass

    @classmethod
    def build_insert(
        cls,
        table_name: str,
        column_list: List[str],
    ) -> str:
        columns = f'({", ".join(column_list)})' if column_list else ''
        values = f'VALUES({", ".join([f"${num}" for num in range(1, len(column_list) + 1)])})' if columns else ''
        return f'INSERT INTO {table_name} {columns} {values} RETURNING *'
