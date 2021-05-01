from pyasync_orm import fields
from pyasync_orm.models import Model


class TestModel:
    def test_init_subclass(self):
        class Foo(Model):
            pass

        assert hasattr(Foo, 'orm')
        assert isinstance(Foo.id, fields.BigInt)
        assert Foo.id.primary_key is True
