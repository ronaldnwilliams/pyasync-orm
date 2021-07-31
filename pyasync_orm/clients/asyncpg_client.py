from contextlib import asynccontextmanager

import asyncpg

from pyasync_orm.clients.abstract_client import AbstractClient


class AsyncPGClient(AbstractClient):
    def __init__(self):
        self.connection_pool = None

    async def create_connection_pool(self, *args, **kwargs):
        self.connection_pool = await asyncpg.create_pool(*args, **kwargs)

    async def close_connection_pool(self):
        await self.connection_pool.close()

    @asynccontextmanager
    async def get_connection(self):
        async with self.connection_pool.acquire() as connection:
            yield connection
