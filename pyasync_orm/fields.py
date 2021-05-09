from typing import Optional, Any


class BaseField:
    def __init__(
            self,
            default: Optional[Any] = None,
            unique: bool = False,
            allow_null: bool = True,
    ):
        self.default = f'DEFAULT {default}' if default else ''
        self.unique = 'UNIQUE' if unique else ''
        self.allow_null = 'NOT NULL' if not allow_null else ''

    def __str__(self):
        return f'{self.default} {self.unique} {self.allow_null}'


class Boolean(BaseField):
    def __str__(self):
        return f'bool {super().__str__()}'.strip()


class Serial(BaseField):
    def __init__(self, primary_key: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.primary_key = 'PRIMARY KEY' if primary_key else ''

    def __str__(self):
        return f'{super().__str__()} {self.primary_key}'.strip()


class SmallInt(Serial):
    def __str__(self):
        return f'smallint {super().__str__()}'.strip()


class Integer(Serial):
    def __str__(self):
        return f'integer {super().__str__()}'.strip()


class BigInt(Serial):
    def __str__(self):
        return f'bigint {super().__str__()}'.strip()


class VarChar(BaseField):
    def __init__(self, max_length: int, **kwargs):
        super().__init__(**kwargs)
        self.max_length = max_length

    def __str__(self):
        return f'varchar ({self.max_length}) {super().__str__()}'.strip()


class Text(BaseField):
    def __str__(self):
        return f'text {super().__str__()}'.strip()


class FixedChar(BaseField):
    def __init__(self, fixed_length, **kwargs):
        super().__init__(**kwargs)
        self.fixed_length = fixed_length

    def __str__(self):
        return f'char ({self.fixed_length}) {super().__str__()}'.strip()


class JSON(BaseField):
    def __str__(self):
        return f'json {super().__str__()}'.strip()


class Date(BaseField):
    def __str__(self):
        return f'date {super().__str__()}'.strip()


class DateTime(BaseField):
    def __init__(
            self,
            with_time_zone: bool = False,
            selected_timezone: str = 'UTC',
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.with_time_zone = 'with time zone' if with_time_zone else ''
        self.selected_timezone = selected_timezone if with_time_zone else None

    def __str__(self):
        return f'timestamp {self.with_time_zone} {super().__str__()}'.strip()


class ByteArray(BaseField):
    def __str__(self):
        return f'bytea {super().__str__()}'.strip()
