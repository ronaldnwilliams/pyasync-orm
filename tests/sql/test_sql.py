import pytest

from pyasync_orm.sql.sql import create_where_string, pop_operator, LOOKUPS, SQL, create_set_columns_string


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
    'where_list,where_sql',
    (
        (['foo'], '(foo = $1)'),
        (['foo__gt'], '(foo > $1)'),
        (['foo', 'foo__gt'], '(foo = $1 AND foo > $2)'),
    )
)
def test_create_where_string(where_list, where_sql):
    where_string = create_where_string(
        where_list=where_list,
        starting_value=1,
    )

    assert where_string == where_sql


def test_create_set_columns_string():
    assert create_set_columns_string(
        set_columns_list=['a'],
        starting_value=1,
    ) == 'a = $1'
    assert create_set_columns_string(
        set_columns_list=['a', 'b'],
        starting_value=1,
    ) == 'a = $1, b = $2'


class TestSQL:
    def test_init(self):
        sql = SQL('foo')

        assert sql.table_name == 'foo'
        assert sql.where == []
        assert sql.order_by == []
        assert sql.limit is None

    def test_add_where(self):
        sql = SQL(table_name='foo')

        sql.add_where(where_list=['a'])

        assert sql.value_count == 1
        assert '(a = $1)' in sql.where

    def test_add_where_exclude(self):
        sql = SQL(table_name='foo')

        sql.add_where(
            where_list=['a', 'b'],
            exclude=True,
        )

        assert sql.value_count == 2
        assert 'NOT (a = $1 AND b = $2)' in sql.where

    def test_add_order_by(self):
        sql = SQL('foo')

        sql.add_order_by(order_by_args_length=2)

        assert sql.value_count == 2
        assert ['$1', '$2'] == sql.order_by

    def test_add_order_by_has_value_count(self):
        sql = SQL('foo')
        sql.value_count = 1

        sql.add_order_by(order_by_args_length=2)

        assert sql.value_count == 3
        assert ['$2', '$3'] == sql.order_by

    def test_set_limit(self):
        sql = SQL('foo')

        sql.set_limit()

        assert sql.value_count == 1
        assert sql.limit == '$1'
