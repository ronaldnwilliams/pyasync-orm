import asyncpg

from pyasync_orm.clients.base import BaseClient


class Client(BaseClient):
    @classmethod
    async def create_pool(cls, url: str) -> asyncpg.Pool:
        return await asyncpg.create_pool(url)
