import pytest

from pyasync_orm.models import Model


class Customer(Model):
    pass


class TestORM:
    def test___init__(self):
        pass

    def test__get_orm(self):
        pass

    @pytest.mark.asyncio
    async def test_create(self):
        customer = await Customer.orm.create()

        assert customer.id == 1

    def test_filter(self):
        pass

    def test_exclude(self):
        pass

    @pytest.mark.asyncio
    async def test_get(self):
        customer = await Customer.orm.create()

        customer_got = await Customer.orm.get(id=customer.id)

        assert customer.id == customer_got.id

    @pytest.mark.asyncio
    async def test_all(self):
        pass

    @pytest.mark.asyncio
    async def test_update(self):
        pass

    @pytest.mark.asyncio
    async def test_delete(self):
        pass
