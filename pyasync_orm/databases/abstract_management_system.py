from abc import ABC, abstractmethod
from typing import Type, TYPE_CHECKING, List

if TYPE_CHECKING:
    from pyasync_orm.databases.abstract_table import AbstractTable


class AbstractManagementSystem(ABC):
    table_class: Type['AbstractTable']
    data_types = {
        'bool': None,
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
    def column_data_sql(cls, table_name: str) -> str:
        pass

    @classmethod
    @abstractmethod
    def index_data_sql(cls, table_name: str) -> str:
        pass

    @classmethod
    @abstractmethod
    def get_create_table_sql(cls, model_table: 'AbstractTable') -> str:
        pass

    @classmethod
    @abstractmethod
    def get_alter_table_sql(
        cls,
        model_table: 'AbstractTable',
        db_table: 'AbstractTable'
    ) -> List[str]:
        pass
