import importlib
from typing import Type
from urllib.parse import urlparse


class Database:
    CLIENTS = {
        'postgresql': 'pyasync_orm.clients.asyncpg'
    }

    def __init__(self):
        self.url = ''
        self.client = None
        self.pool = None

    def get_client(self, client_key: str) -> Type['pyasync_orm.clients.base.BaseClient']:
        client_path = self.CLIENTS.get(client_key)
        if client_path is None:
            raise ValueError(f'Client {client_key} not supported.')
        client_module = importlib.import_module(client_path)
        return client_module.Client

    async def connect(self, url: str):
        self.url = url
        parsed_url = urlparse(url)
        self.client = self.get_client(parsed_url.scheme)
        self.pool = await self.client.create_pool(url)
