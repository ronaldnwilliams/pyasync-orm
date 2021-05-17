from collections import namedtuple
from typing import Optional, Any, TYPE_CHECKING, Union, Type

if TYPE_CHECKING:
    from pyasync_orm.models import Model


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

    def get_sql_string(self):
        return f'{self.default} {self.unique} {self.allow_null}'

    @property
    def field_name_extension(self) -> str:
        return ''


class Boolean(BaseField):
    def get_sql_string(self):
        return f'bool {super().get_sql_string()}'.strip()


class Serial(BaseField):
    def __init__(self, primary_key: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.primary_key = 'PRIMARY KEY' if primary_key else ''

    def get_sql_string(self):
        return f'{super().get_sql_string()} {self.primary_key}'.strip()


class SmallInt(Serial):
    def get_sql_string(self):
        return f'smallint {super().get_sql_string()}'.strip()


class Integer(Serial):
    def get_sql_string(self):
        return f'integer {super().get_sql_string()}'.strip()


class BigInt(Serial):
    def get_sql_string(self):
        return f'bigint {super().get_sql_string()}'.strip()


class VarChar(BaseField):
    def __init__(self, max_length: int, **kwargs):
        super().__init__(**kwargs)
        self.max_length = max_length

    def get_sql_string(self):
        return f'varchar ({self.max_length}) {super().get_sql_string()}'.strip()


class Text(BaseField):
    def get_sql_string(self):
        return f'text {super().get_sql_string()}'.strip()


class FixedChar(BaseField):
    def __init__(self, fixed_length, **kwargs):
        super().__init__(**kwargs)
        self.fixed_length = fixed_length

    def get_sql_string(self):
        return f'char ({self.fixed_length}) {super().get_sql_string()}'.strip()


class JSON(BaseField):
    def get_sql_string(self):
        return f'json {super().get_sql_string()}'.strip()


class Date(BaseField):
    def get_sql_string(self):
        return f'date {super().get_sql_string()}'.strip()


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

    def get_sql_string(self):
        return f'timestamp {self.with_time_zone} {super().get_sql_string()}'.strip()


class ByteArray(BaseField):
    def get_sql_string(self):
        return f'bytea {super().get_sql_string()}'.strip()


# _OnDelete = namedtuple(
#     '_OnDelete',
#     ['CASCADE', 'RESTRICT', 'NO_ACTION', 'SET_NULL', 'SET_DEFAULT'],
#     defaults=['CASCADE', 'RESTRICT', 'NO ACTION', 'SET NULL', 'SET DEFAULT'],
# )

ON_DELETE = namedtuple(
    '_OnDelete',
    ['CASCADE', 'RESTRICT', 'NO_ACTION', 'SET_NULL', 'SET_DEFAULT'],
    defaults=['CASCADE', 'RESTRICT', 'NO ACTION', 'SET NULL', 'SET DEFAULT'],
)()


class RelationshipField(BaseField):
    def __init__(self, model: Union[str, Type['Model']], on_delete: str, **kwargs):
        super().__init__(**kwargs)
        self.model = model
        if on_delete not in set(ON_DELETE._field_defaults.values()):
            raise ValueError(
                f'{self.__class__.__name__} Relationship fields require a proper on delete param. '
                f'Cannot set on delete to: {on_delete}'
            )
        self.on_delete = on_delete


class ForeignKey(RelationshipField):
    def get_sql_string(self):
        return f'bigint REFERENCES {self.model.meta.table_name}'


class ReverseRelationship(RelationshipField):
    def __init__(self, *args, **kwargs):
        self.model_id = None
        super().__init__(*args, **kwargs)

    @property
    def orm(self, **kwargs):
        return self.model.orm

    def all(self):
        return self.model.orm.filter(id=self.model_id).all()
