import inflection

from pyasync_orm.fields import BigIntegerField
from pyasync_orm.orm import ORM


class Model:
    table_name: str
    orm: ORM

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.table_name = inflection.tableize(cls.__name__)
        cls.id = BigIntegerField(primary_key=True, auto_increment=True)
        cls.orm = ORM(model_class=cls)

    def __str__(self):
        return f'<{self.__class__.__name__}: {self.id}>'

    def __repr__(self):
        return f'{self}'

    @classmethod
    def from_db(cls, data: dict) -> 'Model':
        instance = cls()
        for key, value in data.items():
            setattr(instance, key, value)
        return instance
