import inflection

from pyasync_orm.fields import BigSerial
from pyasync_orm.orm import ORM


class Model:
    table_name: str
    orm: ORM

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.table_name = inflection.tableize(cls.__name__)
        cls.id = BigSerial(primary_key=True)
        cls.orm = ORM(model_class=cls)

    def __str__(self):
        return f'<{self.__class__.__name__}: {self.id}>'

    def __repr__(self):
        return f'{self}'

    @classmethod
    def from_db(cls, data: dict) -> 'Model':
        instance = cls()
        for key in data:
            setattr(instance, key, data[key])
        return instance
