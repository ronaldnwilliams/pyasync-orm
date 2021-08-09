from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Type

if TYPE_CHECKING:
    from pyasync_orm.databases.abstract_management_system import AbstractManagementSystem


class AbstractClient(ABC):
    @abstractmethod
    def __init__(self, **db_kwargs):
        pass

    @abstractmethod
    async def create_connection_pool(self, **db_kwargs):
        pass

    @abstractmethod
    async def close_connection_pool(self):
        pass

    @property
    @abstractmethod
    def management_system(self) -> Type['AbstractManagementSystem']:
        pass

    @abstractmethod
    @asynccontextmanager
    async def get_connection(self):
        pass
