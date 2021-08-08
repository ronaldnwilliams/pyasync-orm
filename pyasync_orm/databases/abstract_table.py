from abc import ABC, abstractmethod
from typing import List, Type, TYPE_CHECKING, Any

from pyasync_orm.fields import BaseField

if TYPE_CHECKING:
    from pyasync_orm.databases.abstract_column import AbstractColumn
    from pyasync_orm.models import Model


class AbstractTable(ABC):
    column_class: Type['AbstractColumn']

    def __init__(
        self,
        table_name: str,
        columns: List['AbstractColumn'],
    ):
        self.table_name = table_name
        self.columns = columns

    @classmethod
    def from_model(
        cls,
        model_class: Type['Model']
    ) -> 'AbstractTable':
        return cls(
            table_name=model_class.table_name,
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
    @abstractmethod
    def from_db(
        cls,
        table_name: str,
        column_data: List[Any],
        index_data: List[Any],
    ) -> 'AbstractTable':
        pass
