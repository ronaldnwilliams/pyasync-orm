from contextlib import asynccontextmanager
from typing import Type, TYPE_CHECKING, Union, List, Optional


if TYPE_CHECKING:
    from pyasync_orm.clients.abstract_client import AbstractClient
    from pyasync_orm.databases.abstract_management_system import AbstractManagementSystem
    from pyasync_orm.models import Model


class Database:
    def __init__(self):
        self.client: Optional['AbstractClient'] = None
        self.management_system: Optional[Type['AbstractManagementSystem']] = None
        self.models: Optional[List[Type['Model']]] = None

    async def connect(
        self,
        client: Type['AbstractClient'],
        models: Optional[List[Union[Type['Model'], str]]] = None,
        **db_kwargs,
    ):
        self.client = client(**db_kwargs)
        self.management_system = self.client.management_system
        self.models = models or []
        await self.client.create_connection_pool(**db_kwargs)

    async def close(self):
        await self.client.close_connection_pool()

    @asynccontextmanager
    async def get_connection(self):
        async with self.client.get_connection() as connection:
            yield connection
