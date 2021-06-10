from typing import Optional, List, Type, Dict

import inflection

from pyasync_orm import fields
from pyasync_orm.orm import ORM


class ModelMeta:
    def __init__(
        self,
        model_class: Type['Model'],
    ):
        self.model_class = model_class
        self.table_name = inflection.tableize(model_class.__name__)
        self.table_fields: Dict[str, Type[fields.BaseField]] = {}
        self.select_fields: Dict[str, List[str]] = {self.table_name: []}
        self._setup_meta_data()

    def _setup_meta_data(self):
        for field_name, field_value in self.model_class.__dict__.items():
            if isinstance(field_value, fields.BaseField):
                if getattr(field_value, 'is_db_column', True):
                    self.table_fields[field_name]= field_value
                    self.select_fields[self.table_name].append(f'{self.table_name}.{field_name}')
                elif isinstance(field_value, fields.ForeignKey):
                    self.select_fields.update(field_value.model.meta.select_fields)


class Model:
    id: fields.BigInt
    _foreign_key_name: str
    _reverse_name: str
    meta: ModelMeta
    orm: ORM

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        underscore_name = inflection.underscore(cls.__name__)
        cls._foreign_key_name = f'{underscore_name}_id'
        cls._reverse_name = f'{underscore_name}_set'
        cls.id = fields.BigInt(primary_key=True)
        cls._set_relationships()
        cls.meta = ModelMeta(model_class=cls)
        cls.orm = ORM(cls)

    def __str__(self):
        return f'<{self.__class__.__name__}: {self.id}>'

    def __repr__(self):
        return f'{self}'

    @classmethod
    def _set_relationships(cls):
        field_values = list(cls.__dict__.values())
        for field_value in field_values:
            if isinstance(field_value, fields.ForeignKey):
                setattr(
                    field_value.model,
                    cls._reverse_name,
                    fields.ReverseRelationship(cls, on_delete=field_value.on_delete),
                )
                setattr(
                    cls,
                    field_value.model.foreign_key_name,
                    fields.ForeignKey(field_value.model, on_delete=field_value.on_delete, is_db_column=True),
                )

    async def refresh_from_db(self):
        if isinstance(self.id, int):
            # TODO save the sql_string and values, on the model, in order to make another db call
            refreshed = await self.orm.get(id=self.id)
            self.__dict__.update(refreshed.__dict__)


class ModelSet:
    # TODO is this how I want to handle model sets?
    def __init__(
            self,
            model_class,
            reference_model_instance,
            model_instances: Optional[List[Model]] = None,
    ):
        self.model_class = model_class
        self.reference_model_instance = reference_model_instance
        self._model_instances = model_instances

    @property
    def instances(self) -> List[Model]:
        if self._model_instances is None:
            lookup = {
                f'{self.reference_model_instance.__name__.lower()}_id': self.reference_model_instance.id
            }
            self._model_instances = self.model_class.orm.filter(**lookup).all()
        return self._model_instances

    def all(self) -> List[Model]:
        return self.instances

    # TODO implement a filter method
