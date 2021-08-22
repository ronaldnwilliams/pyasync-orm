from typing import Set

import inflection

from pyasync_orm.fields import BaseField, BigIntegerField
from pyasync_orm.orm import ORM


class Model:
    table_name: str
    orm: ORM
    id: BigIntegerField

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.table_name = inflection.tableize(cls.__name__)
        cls.id = BigIntegerField(primary_key=True, auto_increment=True)
        cls._set_field_names()
        cls.orm = ORM(model_class=cls)

    def __init__(self, **kwargs):
        self._validate_fields(field_names_to_set=set(kwargs.keys()))
        self.orm_fields = kwargs
        for field_name, field_value in kwargs.items():
            setattr(self, field_name, field_value)

    def __str__(self):
        return f'<{self.__class__.__name__}: {self.id}>'

    def __repr__(self):
        return f'{self}'

    @classmethod
    def _set_field_names(cls):
        for name, field_instance in cls.__dict__.items():
            if isinstance(field_instance, BaseField):
                field_instance.name = name

    def _validate_fields(self, field_names_to_set: Set[str]):
        extra_kwargs = set(self.__dict__.keys()) - field_names_to_set
        if extra_kwargs:
            s = 's' if len(extra_kwargs) > 1 else ''
            raise Exception(
                f'{self.__class__.__name__} model does not have field{s}: '
                f'{extra_kwargs}'
            )

    @classmethod
    def from_db(cls, data: dict) -> 'Model':
        instance = cls()
        for key, value in data.items():
            setattr(instance, key, value)
        return instance
