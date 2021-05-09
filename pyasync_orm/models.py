from typing import Type

from pyasync_orm import fields
from pyasync_orm.orm import ORM


class Meta:
    def __init__(self, table_name: str):
        self.table_name = table_name


class Model:
    orm: ORM
    id: fields.BigInt
    meta: Meta

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.meta = Meta(f'{cls.__name__.lower()}s')
        cls.orm = ORM(cls)
        cls.id = fields.BigInt(primary_key=True)

    def __str__(self):
        return f'<{self.__class__.__name__}: {self.id}>'

    def __repr__(self):
        return f'{self}'
