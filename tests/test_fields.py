from pyasync_orm.fields import Serial


class TestSerial:
    def test_init(self):
        class Foo(Serial):
            pass

        foo = Foo()
        foo_primary_key = Foo(primary_key=True)

        assert foo.primary_key is False
        assert foo_primary_key.primary_key is True
