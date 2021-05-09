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
