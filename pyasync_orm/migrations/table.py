from typing import List, Any, Union, Callable, Type, TYPE_CHECKING, Optional


if TYPE_CHECKING:
    from asyncpg import Record
    from pyasync_orm.models import Model


class Column:
    def __init__(
        self,
        column_name: str,
        data_type: Any,  # make classes or enum
        is_nullable: str,  # YES or NO
        unique: bool,
        column_default: Optional[Union[Any, Callable[[Any], Any]]] = None,
        character_maximum_length: Optional[int] = None,
        numeric_precision: Optional[int] = None,
        numeric_scale: Optional[int] = None,
    ):
        self.column_name = column_name
        self.data_type = data_type
        self.column_default = column_default
        self.is_nullable = is_nullable
        self.character_maximum_length = character_maximum_length
        self.unique = unique
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale


class Table:
    columns: List[Column]

    def __init__(self, columns: List[Column]):
        self.columns = columns

    @classmethod
    def from_model(
        cls,
        model_class: Type['Model']
    ) -> 'Table':
        pass

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
            Column(
                unique=data['column_name'] in unique_columns,
                **data,
            )
            for data in column_data
        ]
        return cls(columns=columns)
