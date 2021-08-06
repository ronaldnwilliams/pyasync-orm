import json
from abc import abstractmethod
from typing import Optional, Type, Union, Any, Callable

from pyasync_orm.orm import ORM


class BaseField:
    def __init__(
        self,
        null: bool = True,
        default: Optional[Union[Any, Callable[[], Any]]] = None,
        primary_key: bool = False,
        unique: bool = False,
    ):
        self.null = null
        self.default = default
        self.primary_key = primary_key
        self.unique = unique

    @abstractmethod
    @property
    def data_type(self) -> str:
        pass

    @property
    def db_column_dict(self) -> dict:
        return ORM.database.management_system.column_dict(
            data_type=self.data_type,
            null='YES' if self.null else 'NO',
            unique=self.unique,
            default=self.default,
            max_length=getattr(self, 'max_length', None),
            max_digits=getattr(self, 'max_digits', None),
            decimal_places=getattr(self, 'decimal_places', None),
        )


class SmallSerialField(BaseField):
    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['smallserial']


class SerialField(BaseField):
    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['serial']


class BigSerialField(BaseField):
    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['bigserial']


class SmallIntegerField(BaseField):
    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['smallint']


class IntegerField(BaseField):
    @property
    def data_type(self) -> str:
        return ORM.database.management_system.data_types['integer']


class BigIntegerField(BaseField):
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
            raise ValueError('DateField error; auto_now and auto_now_add. Only set one of these to True')
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
        with_time_zone = ' with time zone' if self.with_time_zone else ''
        return ORM.database.management_system.data_types[f'timestamp{with_time_zone}']
