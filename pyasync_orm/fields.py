import json
from typing import Optional, Type, Union, Any, Callable


class BaseField:
    def __init__(
        self,
        null: bool = True,
        default: Union[Any, Callable[[], Any]] = None,
        primary_key: bool = False,
        unique: bool = False,
    ):
        self.null = null
        self.default = default
        self.primary_key = primary_key
        self.unique = unique


class SmallSerialField(BaseField):
    pass


class SerialField(BaseField):
    pass


class BigSerialField(BaseField):
    pass


class SmallIntegerField(BaseField):
    pass


class IntegerField(BaseField):
    pass


class BigIntegerField(BaseField):
    pass


class PositiveSmallIntegerField(BaseField):
    pass


class PositiveIntegerField(BaseField):
    pass


class PositiveBigIntegerField(BaseField):
    pass


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


class FloatField(BaseField):
    pass


class VarCharField(BaseField):
    def __init__(
        self,
        max_length: int,
        **kwargs,
    ):
        self.max_length = max_length
        super().__init__(**kwargs)


class TextField(BaseField):
    pass


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


class DateTimeField(DateField):
    pass
