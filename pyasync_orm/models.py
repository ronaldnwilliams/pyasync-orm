from pyasync_orm import fields
from pyasync_orm.orm import ORM


class Model:
    orm: ORM
    id: fields.BigInt

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.orm = ORM(cls)
        cls.id = fields.BigInt(primary_key=True)
