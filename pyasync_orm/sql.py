from typing import List


class SQL:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def add_where(self, **kwargs):
        pass

    def add_where_not(self, **kwargs):
        pass

    def build_select(self):
        pass

    def build_update(self):
        pass

    def build_delete(self):
        pass

    @classmethod
    def build_insert(cls, table_name: str, column_list: List[str]) -> str:
        columns = f'({", ".join(column_list)})' if column_list else ''
        values = f'VALUES({", ".join([f"${num}" for num in range(1, len(column_list) + 1)])})' if columns else ''
        return f'INSERT INTO {table_name} {columns} {values} RETURNING *'
