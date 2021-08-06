from typing import List, Any, Union, Callable, Type, TYPE_CHECKING, Optional

from pyasync_orm.databases.abstract_management_system import AbstractManagementSystem
from pyasync_orm.databases.abstract_table import AbstractTable
from pyasync_orm.fields import BaseField

if TYPE_CHECKING:
    from asyncpg import Record
    from pyasync_orm.models import Model


class Table(AbstractTable):
    @classmethod
    def from_model(
        cls,
        model_class: Type['Model']
    ) -> 'Table':
        # TODO left off here
        return cls(
            columns=[
                cls.column_class(
                    column_name=key,
                    **value.db_column_dict,
                )
                for key, value in model_class.__dict__.items()
                if isinstance(value, BaseField)
            ]
        )

    @classmethod
    def _get_column_from_index(
        cls,
        index_name: str,
        table_name: str,
    ) -> str:
        return index_name.removeprefix(f'{table_name}_').removesuffix('_uindex')

    @classmethod
    def from_db(
        cls,
        table_name: str,
        column_data: List['Record'],
        index_names: List['Record'],
    ) -> 'Table':
        unique_columns = [
            cls._get_column_from_index(index_name['indexname'], table_name)
            for index_name in index_names
            if 'uindex' in index_name['indexname']
        ]
        columns = [
            cls.column_class(
                column_name=record['column_name'],
                data_type=record['data_type'],
                null=record['is_nullable'],
                unique=record['column_name'] in unique_columns,
                default=record['column_default'],  # TODO need to translate to python
                max_length=record['character_maximum_length'],
                max_digits=record['numeric_precision'],
                decimal_places=record['numeric_scale'],
            )
            for record in column_data
        ]
        return cls(columns=columns)


class PostgreSQL(AbstractManagementSystem):
    table_class = Table
    data_types = {
        'smallserial': 'smallserial',
        'serial': 'serial',
        'bigserial': 'bigserial',
        'smallint': 'smallint',
        'integer': 'integer',
        'bigint': 'bigint',
        'numeric': 'numeric',
        'double precision': 'double precision',
        'varchar': 'varchar',
        'text': 'text',
        'json': 'json',
        'date': 'date',
        'timestamp': 'timestamp',
        'timestamp with time zone': 'timestamp with time zone',
    }

    @classmethod
    def column_dict(
        cls,
        data_type: str,
        null: bool,
        unique: bool,
        default: Optional[Union[Any, Callable[[], Any]]] = None,
        max_length: Optional[int] = None,
        max_digits: Optional[int] = None,
        decimal_places: Optional[int] = None,
    ) -> dict:
        return {
            'data_type': data_type,
            'is_nullable': 'YES' if null else 'NO',
            'unique': unique,
            'column_default': default,
            'character_maximum_length': max_length,
            'numeric_precision': max_digits,
            'numeric_scale': decimal_places,
        }

    @classmethod
    def column_data_sql(cls, table_name: str) -> str:
        return """
            SELECT
                column_name, column_default, is_nullable,
                data_type, character_maximum_length, numeric_precision,
                numeric_scale
            FROM
                 information_schema.columns
            WHERE
                table_name = '{table_name}';
        """.format(table_name=table_name)

    @classmethod
    def index_names_sql(cls, table_name: str) -> str:
        return """
            SELECT
                indexname
            FROM
                pg_indexes
            WHERE
                tablename = '{table_name}';
        """.format(table_name=table_name)
