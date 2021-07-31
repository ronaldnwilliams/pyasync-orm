from typing import List


class SQL:
    @classmethod
    def build_insert(cls, table_name: str, column_list: List[str]) -> str:
        columns = f'({", ".join(column_list)})' if column_list else ''
        values = f'VALUES({", ".join([f"${num}" for num in range(1, len(column_list) + 1)])})' if columns else ''
        return f'INSERT INTO {table_name} {columns} {values} RETURNING *'
