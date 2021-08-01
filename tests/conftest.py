import asyncio

import pytest

from pyasync_orm.clients.asyncpg_client import AsyncPGClient
from pyasync_orm.orm import ORM


@pytest.fixture(scope='session')
def event_loop(request):
    """Create an instance of the default event loop for each test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session', autouse=True)
async def create_db():
    x = 1
    await ORM.database.connect(
        client=AsyncPGClient,
        dsn='postgresql://postgres@localhost/postgres',
    )
    async with ORM.database.get_connection() as connection:
        await connection.execute(
            'DROP DATABASE IF EXISTS test_async_orm_db'
        )
        await connection.execute(
            'CREATE DATABASE test_async_orm_db'
        )

    await ORM.database.close()
    await ORM.database.connect(
        client=AsyncPGClient,
        dsn='postgresql://postgres@localhost/test_async_orm_db',
    )

    async with ORM.database.get_connection() as connection:
        await connection.execute(
            'CREATE TABLE customers(id BIGSERIAL PRIMARY KEY)'
        )


@pytest.fixture(autouse=True)
async def truncate_tables(create_db):
    x = 1
    async with ORM.database.get_connection() as connection:
        await connection.execute(
            'TRUNCATE customers'
        )
