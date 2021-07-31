from abc import ABC, abstractmethod
from contextlib import asynccontextmanager


class AbstractClient(ABC):
    @abstractmethod
    async def create_connection_pool(self, *args, **kwargs):
        pass

    @abstractmethod
    async def close_connection_pool(self):
        pass

    @abstractmethod
    @asynccontextmanager
    async def get_connection(self):
        pass
