import pytest

from pyasync_orm.models import Model
from pyasync_orm.orm import ORM
from pyasync_orm.sql import SQL


class Customer(Model):
    pass


class TestORM:
    def test___init__(self):
        assert Customer.orm._model_class == Customer
        assert Customer.orm._sql is None

    def test__get_orm(self):
        original_orm = Customer.orm

        new_orm = Customer.orm._get_orm()

        assert original_orm != new_orm

    def test__get_orm_self(self):
        Customer.orm._sql = SQL(table_name=Customer.table_name)
        original_orm = Customer.orm

        same_orm = Customer.orm._get_orm()

        assert original_orm == same_orm
        Customer.orm._sql = None

    @pytest.mark.asyncio
    async def test_create(self):
        customer = await Customer.orm.create()

        assert customer.id == 1

    def test_filter(self):
        orm = Customer.orm.filter(id=1)

        assert isinstance(orm, ORM)

    def test_exclude(self):
        orm = Customer.orm.exclude(id=1)

        assert isinstance(orm, ORM)

    @pytest.mark.asyncio
    async def test_get(self):
        customer = await Customer.orm.create()

        customer_got = await Customer.orm.get(id=customer.id)

        assert customer.id == customer_got.id

    @pytest.mark.asyncio
    async def test_all(self):
        customer_1 = await Customer.orm.create()
        customer_2 = await Customer.orm.create()

        customers = await Customer.orm.all()

        assert len(customers) == 2
        assert [customer_1.id, customer_2.id] == [customer.id for customer in customers]

    @pytest.mark.asyncio
    async def test_update(self):
        # await Customer.orm.create(name='Ron')
        #
        # updated_customers = await Customer.orm.update(name='Ronald')
        #
        # assert updated_customers[0].name == 'Ronald'
        pass

    @pytest.mark.asyncio
    async def test_delete(self):
        customer = await Customer.orm.create()

        customers_deleted = await Customer.orm.delete()

        assert customers_deleted[0].id == customer.id
        assert await Customer.orm.count() == 0

    @pytest.mark.asyncio
    async def test_delete(self):
        assert await Customer.orm.count() == 0

        await Customer.orm.create()

        assert await Customer.orm.count() == 1
