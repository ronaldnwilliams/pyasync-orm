import pytest

from pyasync_orm.sql.base import (
    InsertSQL, pop_operator, LOOKUPS, create_where_string, SelectSQL,
    BaseSQLCommand, create_set_columns_string, UpdateSQL,
)


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


def test_create_set_columns_string():
    assert create_set_columns_string({'a': 1}) == 'a = 1'
    assert create_set_columns_string({'a': 1, 'b': 2}) == 'a = 1, b = 2'


class TestBaseSQLCommand:
    def test_init(self):
        class FooCommand(BaseSQLCommand):
            @property
            def sql_string(self) -> str:
                return 'foo'

        assert FooCommand('foo').table_name == 'foo'


class TestSelect:
    def test_init(self):
        select = SelectSQL(
            table_name='foo',
            columns='*',
        )

        assert select.where is None
        assert select.order_by is None
        assert select.limit is None

    def test_init_with_optionals(self):
        select = SelectSQL(
            table_name='foo',
            columns=['a', 'b'],
            where={'a': 1},
            order_by=['b', 'a'],
            limit=1,
        )

        assert select.where == 'a = 1'
        assert select.order_by == 'b, a'
        assert select.limit == 1

    def test_sql_string(self):
        select = SelectSQL(
            table_name='foo',
            columns='*',
        )

        assert select.sql_string == 'SELECT * FROM foo '

    def test_sql_string_with_optionals(self):
        select = SelectSQL(
            table_name='foo',
            columns=['a', 'b'],
            where={'a': 1},
            order_by=['b', 'a'],
            limit=1,
        )

        assert select.sql_string == (
            'SELECT a, b FROM foo '
            'WHERE a = 1 '
            'ORDER BY b, a '
            'LIMIT 1'
        )


class TestInsert:
    def test_init(self):
        insert = InsertSQL(
            table_name='foo',
            columns=['a', 'b', 'c'],
        )

        assert insert.table_name == 'foo'
        assert insert.columns == 'a, b, c'
        assert insert.returning is None

    def test_init_with_optionals(self):
        insert_returning_string = InsertSQL(
            table_name='foo',
            columns=['a', 'b', 'c'],
            returning='*',
        )
        insert_returning_list = InsertSQL(
            table_name='foo',
            columns=['a', 'b', 'c'],
            returning=['a', 'b'],
        )

        # insert_returning_string
        assert insert_returning_string.table_name == 'foo'
        assert insert_returning_string.columns == 'a, b, c'
        assert insert_returning_string.returning == '*'
        # insert_returning_list
        assert insert_returning_list.table_name == 'foo'
        assert insert_returning_list.columns == 'a, b, c'
        assert insert_returning_list.returning == 'a, b'

    def test_sql_string(self):
        columns = ['a', 'b', 'c']
        expected_values = ", ".join(str(index) for index in range(1, len(columns) + 1))

        insert = InsertSQL(
            table_name='foo',
            columns=columns,
        )

        assert insert.sql_string == (
            f'INSERT INTO foo (a, b, c) VALUES ({expected_values})'
        )

    def test_sql_string_with_optionals(self):
        columns = ['a', 'b', 'c']
        expected_values = ", ".join(str(index) for index in range(1, len(columns) + 1))

        insert = InsertSQL(
            table_name='foo',
            columns=columns,
            returning='*',
        )

        assert insert.sql_string == (
            f'INSERT INTO foo (a, b, c) VALUES ({expected_values}) RETURNING *'
        )


class TestUpdate:
    def test_init(self):
        update = UpdateSQL(
            table_name='foo',
            set_columns={'a': 1},
        )

        assert update.table_name == 'foo'
        assert update.set_columns == 'a = 1'
        assert update.where is None
        assert update.returning is None

    def test_init_with_options(self):
        set_columns = {'a': 1}
        where = {'a': 2}

        update_returning_string = UpdateSQL(
            table_name='foo',
            set_columns=set_columns,
            where=where,
            returning='*'
        )
        update_returning_list = UpdateSQL(
            table_name='foo',
            set_columns=set_columns,
            where=where,
            returning=['a']
        )

        # update_returning_string
        assert update_returning_string.table_name == 'foo'
        assert update_returning_string.set_columns == 'a = 1'
        assert update_returning_string.where == 'a = 2'
        assert update_returning_string.returning == '*'
        # update_returning_list
        assert update_returning_list.table_name == 'foo'
        assert update_returning_list.set_columns == 'a = 1'
        assert update_returning_list.where == 'a = 2'
        assert update_returning_list.returning == 'a'

    def test_sql_string(self):
        update_sql_string = UpdateSQL(
            table_name='foo',
            set_columns={'a': 1},
        ).sql_string

        assert update_sql_string == 'UPDATE foo SET a = 1 '

    def test_sql_string_with_optionals(self):
        update_sql_string = UpdateSQL(
            table_name='foo',
            set_columns={'a': 1},
            where={'a': 2},
            returning='*',
        ).sql_string

        assert update_sql_string == (
            'UPDATE foo SET a = 1 WHERE a = 2 RETURNING *'
        )
