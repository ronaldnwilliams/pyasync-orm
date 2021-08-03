from typing import List, Any, Union, Callable, Type, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyasync_orm.models import Model


class Column:
    column_name: str
    data_type: Any  # make classes or enum
    column_default: Optional[Union[Any, Callable[[Any], Any]]]
    is_nullable: Any  # YES or NO
    character_maximum_length: Optional[int]


class Table:
    columns: List[Column]

    @classmethod
    def from_model(
        cls,
        model_class: Type['Model']
    ) -> 'Table':
        pass

    @classmethod
    def from_db(
        cls,
        table_description: str
    ) -> 'Table':
        pass
