from contextlib import asynccontextmanager
from typing import Type, TYPE_CHECKING

import asyncpg

from pyasync_orm.clients.abstract_client import AbstractClient
from pyasync_orm.databases.postgresql import PostgreSQL

if TYPE_CHECKING:
    from pyasync_orm.databases.abstract_management_system import AbstractManagementSystem


class AsyncPGClient(AbstractClient):
    def __init__(self, **db_kwargs):
        self.connection_pool = None
        self.data_types = PostgreSQL.data_types

    @property
    def management_system(self) -> Type['AbstractManagementSystem']:
        return PostgreSQL

    async def create_connection_pool(self, **db_kwargs):
        self.connection_pool = await asyncpg.create_pool(**db_kwargs)

    async def close_connection_pool(self):
        await self.connection_pool.close()

    @asynccontextmanager
    async def get_connection(self):
        async with self.connection_pool.acquire() as connection:
            yield connection
