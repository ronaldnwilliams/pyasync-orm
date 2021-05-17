from abc import ABC, abstractmethod
from typing import List, Optional, Union


class BaseSQLCommand(ABC):
    def __init__(
        self,
        # TODO table_name might need to become Union[str, List[str]] to handle table relationships
        table_name: str,
    ):
        self.table_name = table_name

    @property
    @abstractmethod
    def sql_string(self) -> str:
        ...

    @classmethod
    def format_wheres_string(
            cls,
            wheres: Union[List[str], None],
    ) -> Union[str, None]:
        if wheres:
            wheres = ' AND '.join(wheres)
        return wheres

    @classmethod
    def format_returning_string(
            cls,
            returning: Union[str, List[str], None],
    ) -> Union[str, None]:
        if isinstance(returning, list):
            returning = ", ".join(column for column in returning)
        return returning


class SelectSQL(BaseSQLCommand):
    def __init__(
        self,
        table_name: str,
        columns: str,
        where: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ):
        super().__init__(table_name)
        self.columns = columns
        self.where = self.format_wheres_string(where)
        if order_by:
            # TODO handle DESC
            order_by = ", ".join(order for order in order_by)
        self.order_by = order_by
        self.limit = limit

    @property
    def sql_string(self) -> str:
        _sql = (
            f'SELECT {self.columns} '
            f'FROM {self.table_name} '
        )
        if self.where:
            _sql += f'WHERE {self.where} '
        if self.order_by:
            _sql += f'ORDER BY {self.order_by} '
        if self.limit:
            _sql += f'LIMIT {self.limit}'
        return _sql


class InsertSQL(BaseSQLCommand):
    def __init__(
        self,
        table_name: str,
        columns: List[str],
        returning: Optional[Union[str, List[str]]] = None,
    ):
        super().__init__(table_name)
        if columns:
            self.columns = '(' + ', '.join(column for column in columns) + ')'
            self.values = ', '.join(f'${index}' for index in range(1, len(columns) + 1))
        else:
            self.columns = ''
            self.values = 'default'
        self.returning = self.format_returning_string(returning)

    @property
    def sql_string(self) -> str:
        _sql = (
            f'INSERT INTO {self.table_name} '
            f'{self.columns} '
            f'VALUES ({self.values})'
        )
        if self.returning:
            _sql += f' RETURNING {self.returning}'
        return _sql


class UpdateSQL(BaseSQLCommand):
    def __init__(
        self,
        table_name: str,
        set_columns_string: str,
        where: Optional[List[str]] = None,
        returning: Optional[Union[str, List[str]]] = None,
    ):
        super().__init__(table_name)
        self.set_columns_string = set_columns_string
        self.where = self.format_wheres_string(where)
        self.returning = self.format_returning_string(returning)

    @property
    def sql_string(self) -> str:
        _sql = (
            f'UPDATE {self.table_name} '
            f'SET {self.set_columns_string} '
        )
        if self.where:
            _sql += f'WHERE {self.where} '
        if self.returning:
            _sql += f'RETURNING {self.returning}'
        return _sql


class DeleteSQL(BaseSQLCommand):
    def __init__(
        self,
        table_name: str,
        where: Optional[List[str]] = None,
        returning: Optional[Union[str, List[str]]] = None,
    ):
        super().__init__(table_name)
        self.where = self.format_wheres_string(where)
        self.returning = self.format_returning_string(returning)

    @property
    def sql_string(self) -> str:
        _sql = (
            f'DELETE FROM {self.table_name} '
        )
        if self.where:
            _sql += f'WHERE {self.where} '
        if self.returning:
            _sql += f'RETURNING {self.returning}'
        return _sql
