import inflection

from pyasync_orm.orm import ORM


class Model:
    orm: ORM

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.table_name = inflection.tableize(cls.__name__)
        cls.orm = ORM(model_class=cls)

    def __str__(self):
        return f'<{self.__class__.__name__}: {self.id}>'

    def __repr__(self):
        return f'{self}'
