from typing import List, TYPE_CHECKING, Optional, Union, Any, Callable

from pyasync_orm.databases.abstract_column import AbstractColumn
from pyasync_orm.databases.abstract_management_system import AbstractManagementSystem
from pyasync_orm.databases.abstract_table import AbstractTable

if TYPE_CHECKING:
    from asyncpg import Record


class DefaultDataType:
    def __init__(
        self,
        column_name: str,
        data_type: str,
        null: bool,
        unique: bool,
        primary_key: bool,
        auto_increment: bool,
        default: Optional[Union[Any, Callable[[], Any]]],
        max_digits: Optional[int],
        decimal_places: Optional[int],
        max_length: Optional[int],
    ):
        self.column_name = column_name
        self.data_type = data_type
        self.null = ' NOT NULL' if not null else ''
        self.unique = ' UNIQUE' if unique else ''
        self.default = f' DEFAULT {default}' if default is not None else ''
        self.max_digits = max_digits
        self.decimal_places = decimal_places
        self.max_length = max_length
        self.primary_key = primary_key
        self.auto_increment = auto_increment

    def __str__(self):
        return f'{self.column_name} {self.data_type}{self.null}{self.unique}{self.default}'


class CharVarDataType(DefaultDataType):
    def __str__(self):
        self.data_type = f'{self.data_type}({self.max_length})'
        return super().__str__()


class NumericDataType(DefaultDataType):
    def __str__(self):
        self.data_type = f'{self.data_type}({self.max_digits}, {self.decimal_places})'
        return super().__str__()


class IntDataType(DefaultDataType):
    serial_data_types = {
        'smallint': 'smallserial',
        'integer': 'serial',
        'bigint': 'bigserial',
    }

    def __str__(self):
        if self.auto_increment:
            primary_key = ' PRIMARY KEY' if self.primary_key else ''
            string_ = f'{self.column_name} {self.serial_data_types[self.data_type]}{primary_key}'
        else:
            string_ = super().__str__()
        return string_


class Column(AbstractColumn):
    data_type_classes = {
        'default': DefaultDataType,
        'character varying': CharVarDataType,
        'numeric': NumericDataType,
        'smallint': IntDataType,
        'integer': IntDataType,
        'bigint': IntDataType,
    }

    def __str__(self):
        data_type_class = self.data_type_classes.get(self.data_type, DefaultDataType)
        return str(data_type_class(
            column_name=self.column_name,
            data_type=self.data_type,
            null=self.null,
            unique=self.unique,
            default=self.default,
            max_digits=self.max_digits,
            decimal_places=self.decimal_places,
            max_length=self.max_length,
            primary_key=self.primary_key,
            auto_increment=self.auto_increment,
        ))


class Table(AbstractTable):
    column_class = Column

    @classmethod
    def _get_column_name_from_index(cls, index_definition: str) -> str:
        range_start = index_definition.find('(') + 1
        range_finish = index_definition.find(')')
        return index_definition[range_start: range_finish]

    @classmethod
    def from_db(
        cls,
        table_name: str,
        column_data: List['Record'],
        index_data: List['Record'],
    ) -> 'Table':
        unique_columns = [
            cls._get_column_name_from_index(data['indexdef'])
            for data in index_data
            if 'uindex' in data['indexname'].split('_')[-1]
        ]
        primary_key_columns = [
            cls._get_column_name_from_index(data['indexdef'])
            for data in index_data
            if 'pkey' in data['indexname'].split('_')[-1]
        ]
        columns = [
            cls.column_class(
                column_name=record['column_name'],
                data_type=record['data_type'],
                null=record['is_nullable'] == 'YES',
                primary_key=record['column_name'] in primary_key_columns,
                auto_increment=False if record['column_default'] is None else 'nextval(' in record['column_default'],
                unique=record['column_name'] in unique_columns + primary_key_columns,
                default=record['column_default'],  # TODO need to translate to python
                max_length=record['character_maximum_length'],
                max_digits=record['numeric_precision'],
                decimal_places=record['numeric_scale'],
            )
            for record in column_data
        ]
        return cls(table_name=table_name, columns=columns)

    def __sub__(self, other: 'Table') -> 'Table':
        other_column_names = {column.column_name for column in other.columns}
        add_columns = [column for column in self.columns if column.column_name not in other_column_names]
        return Table(table_name=self.table_name, columns=add_columns)


class PostgreSQL(AbstractManagementSystem):
    table_class = Table
    data_types = {
        'bool': 'boolean',
        'smallserial': 'smallserial',
        'serial': 'serial',
        'bigserial': 'bigserial',
        'smallint': 'smallint',
        'integer': 'integer',
        'bigint': 'bigint',
        'numeric': 'numeric',
        'double precision': 'double precision',
        'varchar': 'character varying',
        'text': 'text',
        'json': 'json',
        'date': 'date',
        'timestamp': 'timestamp',
        'timestamp with time zone': 'timestamp with time zone',
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
    def index_data_sql(cls, table_name: str) -> str:
        return """
            SELECT
                indexname, indexdef
            FROM
                pg_indexes
            WHERE
                tablename = '{table_name}';
        """.format(table_name=table_name)

    @classmethod
    def get_create_table_sql(cls, model_table: 'AbstractTable') -> str:
        return (
            f'CREATE TABLE {model_table.table_name}'
            f'({", ".join([str(column) for column in model_table.columns])})'
        )

    @classmethod
    def _get_add_columns_sql(cls, table: Table) -> List[str]:
        table_name = table.table_name
        return [
            f'ALTER TABLE {table_name} ADD COLUMN {column}'
            for column in table.columns
        ]

    @classmethod
    def _get_drop_columns_sql(cls, table: Table) -> List[str]:
        table_name = table.table_name
        return [
            f'ALTER TABLE {table_name} DROP COLUMN {column.column_name}'
            for column in table.columns
        ]

    @classmethod
    def get_alter_table_sql(
        cls,
        model_table: 'AbstractTable',
        db_table: 'AbstractTable'
    ) -> List[str]:
        # TODO check for column renames
        add_columns_table = model_table - db_table
        drop_columns_table = db_table - model_table
        # TODO check for column updates
        add_sql_list = cls._get_add_columns_sql(table=add_columns_table)
        drop_sql_list = cls._get_drop_columns_sql(table=drop_columns_table)
        return add_sql_list + drop_sql_list
