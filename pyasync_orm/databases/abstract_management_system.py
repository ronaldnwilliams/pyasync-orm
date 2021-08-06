from abc import ABC, abstractmethod
from ctypes import Union
from typing import Optional, Any, Callable, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from pyasync_orm.databases.abstract_table import AbstractTable


class AbstractManagementSystem(ABC):
    table_class: Type['AbstractTable']
    data_types = {
        'smallserial': None,
        'serial': None,
        'bigserial': None,
        'smallint': None,
        'integer': None,
        'bigint': None,
        'numeric': None,
        'double precision': None,
        'varchar': None,
        'text': None,
        'json': None,
        'date': None,
        'timestamp': None,
        'timestamp with time zone': None,
    }

    @classmethod
    @abstractmethod
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
        pass

    @classmethod
    @abstractmethod
    def column_data_sql(cls, table_name: str) -> str:
        pass

    @classmethod
    @abstractmethod
    def index_names_sql(cls, table_name: str) -> str:
        pass
