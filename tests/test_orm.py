import pytest

from pyasync_orm.models import Model
from pyasync_orm.orm import ORM
from pyasync_orm.sql.sql import SQL


class Foo(Model):
    pass


@pytest.fixture
def get_foo_sql():
    sql = SQL('foo')
    Foo.orm._sql = sql
    yield sql
    Foo.orm._sql = None


class TestORM:
    def test_init(self):
        assert Foo.orm.model_class == Foo
        assert Foo.orm._sql is None
        assert Foo.orm._values == ()

    def test_get_orm(self):
        orm = Foo.orm._get_orm()

        assert isinstance(orm, ORM)
        assert Foo.orm.model_class == Foo
        orm_sql = orm._sql
        assert isinstance(orm_sql, SQL)
        assert orm._sql.table_name == Foo.meta.table_name

        orm_again = orm._get_orm()

        assert orm_again == orm
        assert orm_again._sql == orm_sql

    def test_sql(self):
        assert Foo.orm._sql is None
        assert isinstance(Foo.orm._get_sql, SQL)

    def test_sql_already_set(self, get_foo_sql):
        sql = get_foo_sql

        assert Foo.orm._get_sql == sql

    def test_filter(self):
        orm = Foo.orm.filter(tacos=1)

        assert orm != Foo.orm
        assert orm._sql.where == ['(tacos = $1)']
        assert orm._values == (1, )

    def test_exclude(self):
        orm = Foo.orm.exclude(tacos=1)

        assert orm != Foo.orm
        assert orm._sql.where == ['NOT (tacos = $1)']
        assert orm._values == (1, )

    def test_order_by(self):
        orm = Foo.orm.order_by('tacos')

        assert orm != Foo.orm
        assert orm._sql.order_by == ['$1']
        assert orm._values == ('tacos', )

    def test_limit(self):
        orm = Foo.orm.limit(5)

        assert orm != Foo.orm
        assert orm._sql.limit == '$1'
        assert orm._values == (5, )
