from typing import Optional, List, Type, Dict

import inflection

from pyasync_orm import fields
from pyasync_orm.orm import ORM


class Meta:
    def __init__(
        self,
        table_name: str,
        foreign_key_name: str,
        reverse_name: str,
    ):
        self.table_name = table_name
        self.foreign_key_name = foreign_key_name
        self.reverse_name = reverse_name
        self.table_fields: Dict[str, Type[fields.BaseField]] = {}


class Model:
    orm: ORM
    id: fields.BigInt
    meta: Meta

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.id = fields.BigInt(primary_key=True)
        underscore_name = inflection.underscore(cls.__name__)
        cls.meta = Meta(
            table_name=inflection.tableize(cls.__name__),
            foreign_key_name=f'{underscore_name}_id',
            reverse_name=f'{underscore_name}_set',
        )
        cls._set_relationships()
        cls.meta.table_fields = {
            k: v
            for k, v in cls.__dict__.items()
            if isinstance(v, fields.BaseField)
        }
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
                    cls.meta.reverse_name,
                    fields.ReverseRelationship(cls, on_delete=field_value.on_delete),
                )
                setattr(
                    cls,
                    field_value.model.meta.foreign_key_name,
                    fields.ForeignKey(field_value.model, on_delete=field_value.on_delete, is_db_column=True),
                )

    def _update_reverse_relationships(self, record):
        reverse_relationship_fields = tuple(
            (field_key, field_value, )
            for field_key, field_value in self.__dict__.items()
            if isinstance(field_value, fields.ReverseRelationship)
        )
        for field in reverse_relationship_fields:
            # TODO check record for kwargs
            field_key, field_value = field
            setattr(
                self,
                field_key,
                ModelSet(field_value.model, self),
            )

    def update_relationships(self, record: dict):
        self._update_reverse_relationships(record)

    async def refresh_from_db(self):
        if isinstance(self.id, int):
            refreshed = await self.orm.get(id=self.id)
            self.__dict__.update(refreshed.__dict__)


class ModelSet:
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
