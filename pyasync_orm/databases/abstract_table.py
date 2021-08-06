from abc import ABC, abstractmethod
from typing import List, Type, TYPE_CHECKING, Any

from pyasync_orm.databases.column import Column

if TYPE_CHECKING:
    from pyasync_orm.models import Model


class AbstractTable(ABC):
    column_class = Column

    def __init__(self, columns: List[Column]):
        self.columns = columns

    @classmethod
    @abstractmethod
    def from_model(
        cls,
        model_class: Type['Model']
    ) -> 'AbstractTable':
        pass

    @classmethod
    @abstractmethod
    def from_db(
        cls,
        table_name: str,
        column_data: List[Any],
        index_names: List[Any],
    ) -> 'AbstractTable':
        pass
