import json
from abc import abstractmethod, ABC
from enum import Enum
from typing import Optional, Type, Union, Any, Callable

from pyasync_orm.orm import ORM


class Symbol(Enum):
    LESS_THAN = '<'
    LESS_THAN_OR_EQUAL_TO = '<='
    EQUAL_TO = '='
    NOT_EQUAL_TO = '!='
    GREATER_THAN = '>'
    GREATER_THAN_OR_EQUAL_TO = '>='


class SearchCondition:
    def __init__(self, field_name: str, symbol: Symbol, field_value: Any):
        self.field_name = field_name
        self.symbol: str = symbol.value
        self.field_value = field_value


class BaseField(ABC):
    def __init__(
        self,
        null: bool = True,
        default: Optional[Union[Any, Callable[[], Any]]] = None,
        unique: bool = False,
        primary_key: bool = False,
    ):
        self.null = null
        self.default = default
        self.unique = unique
        self.primary_key = primary_key
        if self.primary_key:
            self.null = False
            self.default = None
            self.unique = True
        self.name = ''  # set by Model.__init_subclass__

    def __lt__(self, value: Any) -> SearchCondition:
        return SearchCondition(
            field_name=self.name,
            symbol=Symbol.LESS_THAN,
            field_value=value,
        )

    def __le__(self, value: Any) -> SearchCondition:
        return SearchCondition(
            field_name=self.name,
            symbol=Symbol.LESS_THAN_OR_EQUAL_TO,
            field_value=value,
        )

    def __eq__(self, value: Any) -> SearchCondition:
        return SearchCondition(
            field_name=self.name,
            symbol=Symbol.EQUAL_TO,
            field_value=value,
        )

    def __ne__(self, value: Any) -> SearchCondition:
        return SearchCondition(
            field_name=self.name,
            symbol=Symbol.NOT_EQUAL_TO,
            field_value=value,
        )

    def __gt__(self, value: Any) -> SearchCondition:
        return SearchCondition(
            field_name=self.name,
            symbol=Symbol.GREATER_THAN,
            field_value=value,
        )

    def __ge__(self, value: Any) -> SearchCondition:
        return SearchCondition(
            field_name=self.name,
            symbol=Symbol.GREATER_THAN_OR_EQUAL_TO,
            field_value=value,
        )

    @property
    @abstractmethod
    def data_type(self) -> str:
        pass

    @property
    def db_column_dict(self) -> dict:
        return {
            'data_type': self.data_type,
            'null': self.null,
            'unique': self.unique,
            'default': self.default,
            'primary_key': self.primary_key,
            'auto_increment': getattr(self, 'auto_increment', False),
            'max_length': getattr(self, 'max_length', None),
            'max_digits': getattr(self, 'max_digits', None),
            'decimal_places': getattr(self, 'decimal_places', None),
        }


class BaseIntField(BaseField, ABC):
    def __init__(
        self,
        auto_increment: bool = False,
        **kwargs,
    ):
        self.decimal_places = 0
        self.auto_increment = auto_increment
        super().__init__(**kwargs)


class SmallIntegerField(BaseIntField):
    def __init__(self, **kwargs):
        self.max_digits = 16
        super().__init__(**kwargs)

    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['smallint']


class IntegerField(BaseIntField):
    def __init__(self, **kwargs):
        self.max_digits = 32
        super().__init__(**kwargs)

    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['integer']


class BigIntegerField(BaseIntField):
    def __init__(self, **kwargs):
        self.max_digits = 64
        super().__init__(**kwargs)

    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['bigint']


class DecimalField(BaseField):
    def __init__(
        self,
        max_digits: int,
        decimal_places: int,
        **kwargs,
    ):
        self.max_digits = max_digits
        self.decimal_places = decimal_places
        super().__init__(**kwargs)

    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['numeric']


class FloatField(BaseField):
    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['double precision']


class BooleanField(BaseField):
    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['bool']


class VarCharField(BaseField):
    def __init__(
        self,
        max_length: int,
        **kwargs,
    ):
        self.max_length = max_length
        super().__init__(**kwargs)

    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['varchar']


class TextField(BaseField):
    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['text']


class JSONField(BaseField):
    def __init__(
        self,
        encoder: Optional[Type[json.JSONEncoder]] = None,
        decoder: Optional[Type[json.JSONDecoder]] = None,
        **kwargs,
    ):
        self.encoder = encoder or json.JSONEncoder
        self.decoder = decoder or json.JSONDecoder
        super().__init__(**kwargs)

    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['json']


class DateField(BaseField):
    def __init__(
        self,
        auto_now: bool = False,
        auto_now_add: bool = False,
        **kwargs,
    ):
        if auto_now and auto_now_add:
            raise ValueError(
                'DateField error; auto_now and auto_now_add. '
                'Only set one of these to True')
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add
        super().__init__(**kwargs)

    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['date']


class DateTimeField(DateField):
    def __init__(self, with_time_zone=False, **kwargs):
        self.with_time_zone = with_time_zone
        super().__init__(**kwargs)

    @property
    def data_type(self) -> str:
        timestamp = (
            'timestamp' if self.with_time_zone else 'timestamp with time zone'
        )
        return ORM.database.management_system.data_types[timestamp]
