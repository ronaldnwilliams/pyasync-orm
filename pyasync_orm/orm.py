from typing import TYPE_CHECKING, Type

from pyasync_orm.database import Database
from pyasync_orm.sql import SQL

if TYPE_CHECKING:
    from pyasync_orm.models import Model


class ORM:
    database = Database()

    def __init__(self, model_class: Type['Model']):
        self.model_class = model_class

    async def create(self, **kwargs):
        sql = SQL.build_insert(self.model_class.table_name, list(kwargs.keys()))
        async with self.database.get_connection() as connection:
            result = await connection.fetch(sql, *kwargs.values())
        return self.model_class.construct(**result[0])

    async def get(self):
        pass

    async def all(self):
        pass

    async def update(self):
        pass

    async def delete(self):
        pass
