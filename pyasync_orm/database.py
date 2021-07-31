from contextlib import asynccontextmanager
from typing import Type, TYPE_CHECKING

if TYPE_CHECKING:
    from pyasync_orm.clients.abstract_client import AbstractClient


class Database:
    def __init__(self):
        self.client = None

    async def connect(self, client: Type['AbstractClient'], *args, **kwargs):
        self.client = client()
        await self.client.create_connection_pool(*args, **kwargs)

    async def close(self):
        await self.client.close_connection_pool()

    @asynccontextmanager
    async def get_connection(self):
        async with self.client.get_connection() as connection:
            yield connection
