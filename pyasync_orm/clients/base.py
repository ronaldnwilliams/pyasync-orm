from abc import ABC, abstractmethod
from typing import Type


class BaseTransaction(ABC):
    pass


class BaseConnection(ABC):
    @abstractmethod
    def transaction(self) -> Type[BaseTransaction]:
        ...


class BasePoolAcquireConnection(ABC):
    @abstractmethod
    async def __aenter__(self) -> Type[BaseConnection]:
        ...

    @abstractmethod
    async def __aexit__(self, *exc):
        ...


class BasePool(ABC):
    @abstractmethod
    async def acquire(self) -> Type[BasePoolAcquireConnection]:
        ...


class BaseClient(ABC):
    @classmethod
    @abstractmethod
    async def create_pool(cls, url: str) -> Type[BasePool]:
        ...
