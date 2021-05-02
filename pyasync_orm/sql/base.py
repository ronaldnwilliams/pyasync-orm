from typing import List, Optional, Union


class Insert:
    def __init__(
        self,
        table_name: str,
        columns: List[str],
        returning: Optional[Union[str, List[str]]] = None,
    ):
        self.table_name = table_name
        self.columns = ", ".join(column for column in columns)
        self.values = ", ".join(str(index) for index in range(1, len(columns) + 1))
        if isinstance(returning, list):
            returning = ", ".join(column for column in returning)
        self.returning = returning

    @property
    def sql_string(self) -> str:
        _sql = (
            f'INSERT INTO {self.table_name} '
            f'({self.columns}) '
            f'VALUES ({self.values})'
        )
        if self.returning:
            _sql += f' RETURNING {self.returning};'
        else:
            _sql += ';'
        return _sql


class Select:
    pass


class Update:
    pass


class Delete:
    pass
