import pytest

from pyasync_orm.sql.sql import create_where_string, pop_operator, LOOKUPS, SQL


@pytest.mark.parametrize(
    'lookup_key,lookup_value',
    tuple((key, value) for key, value in LOOKUPS.items()),
)
def test_pop_operator(lookup_key, lookup_value):
    key_part = 'foo'

    key_parts, operator = pop_operator(f'{key_part}__{lookup_key}')

    assert key_parts[0] == key_part
    assert operator == lookup_value


def test_pop_operator_none():
    key_part = 'foo'

    key_parts, operator = pop_operator(key_part)

    assert key_parts[0] == key_part
    assert operator == '='


@pytest.mark.parametrize(
    'where_dict,where_sql',
    (
        ({'foo': 1}, 'foo = 1'),
        ({'foo__gt': 2}, 'foo > 2'),
        ({'foo': 1, 'foo__gt': 2}, 'foo = 1 AND foo > 2'),
    )
)
def test_create_where_clause(where_dict, where_sql):
    where_string = create_where_string(where_dict)

    assert where_string == where_sql


class TestSQL:
    def test_init(self):
        sql = SQL('foo')

        assert sql.table_name == 'foo'
        assert sql.where == []
        assert sql.order_by == []
        assert sql.limit is None

    def test_add_where(self):
        sql = SQL('foo')

        sql.add_where({'a': 1})

        assert 'a = 1' in sql.where

    def test_add_where_exclude(self):
        sql = SQL('foo')

        sql.add_where({'a': 1}, exclude=True)

        assert 'NOT (a = 1)' in sql.where

    def test_add_order_by(self):
        sql = SQL('foo')

        sql.add_order_by(('a', 'b'))

        assert ['a', 'b'] == sql.order_by

    def test_set_limit(self):
        sql = SQL('foo')

        sql.set_limit(1)

        assert sql.limit == 1
