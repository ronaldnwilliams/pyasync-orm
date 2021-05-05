from typing import Optional, Tuple

from pyasync_orm.sql.base import SQL


class ORM:
    def __init__(self, model_class, _sql: Optional[SQL] = None):
        self.model_class = model_class
        self._sql = _sql

    def _get_orm(self):
        if self._sql is None:
            orm = ORM(self.model_class, SQL(self.model_class.__table_name))
        else:
            orm = self
        return orm

    async def filter(self, __sql: Optional[SQL] = None, **kwargs) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_where(kwargs)
        return orm

    async def exclude(self, **kwargs):
        orm = self._get_orm()
        orm._sql.add_where(kwargs, exclude=True)
        return orm

    async def oder_by(self, *args: Tuple[str]):
        orm = self._get_orm()
        orm._sql.add_order_by(args)
        return orm

    async def limit(self, number: int):
        orm = self._get_orm()
        orm._sql.set_limit(number)
        return orm

    async def count(self):
        pass

    async def create(self, **kwargs):
        pass

    async def bulk_create(self, **kwargs):
        pass

    async def get(self, **kwargs):
        pass

    async def all(self):
        pass

    async def update(self, **kwargs):
        pass

    async def bulk_update(self, **kwargs):
        pass

    async def delete(self):
        pass
