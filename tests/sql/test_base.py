import pytest

from pyasync_orm.sql.base import Insert, pop_operator, LOOKUPS, create_where_string, Select


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


class TestSelect:
    def test_init(self):
        select = Select(
            table_name='foo',
            columns='*',
        )

        assert select.where is None
        assert select.order_by is None
        assert select.limit is None

    def test_init_with_optionals(self):
        select = Select(
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
        select = Select(
            table_name='foo',
            columns='*',
        )

        assert select.sql_string == 'SELECT * FROM foo '

    def test_sql_string_with_optionals(self):
        select = Select(
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
        table_name = 'foo'
        columns = ['a', 'b', 'c']
        expected_columns = 'a, b, c'

        insert = Insert(
            table_name=table_name,
            columns=columns,
        )

        assert insert.table_name == table_name
        assert insert.columns == expected_columns
        assert insert.returning is None

    def test_init_returning_str(self):
        table_name = 'foo'
        columns = ['a', 'b', 'c']
        expected_columns = 'a, b, c'
        returning_string = '*'

        insert = Insert(
            table_name=table_name,
            columns=columns,
            returning=returning_string,
        )

        assert insert.table_name == table_name
        assert insert.columns == expected_columns
        assert insert.returning == returning_string

    def test_init_returning_list(self):
        table_name = 'foo'
        columns = ['a', 'b', 'c']
        expected_columns = 'a, b, c'
        returning_list = ['a', 'b']
        expected_returning_list = 'a, b'

        insert = Insert(
            table_name=table_name,
            columns=columns,
            returning=returning_list,
        )

        assert insert.table_name == table_name
        assert insert.columns == expected_columns
        assert insert.returning == expected_returning_list

    def test_sql_string(self):
        table_name = 'foo'
        columns = ['a', 'b', 'c']
        expected_columns = 'a, b, c'
        expected_values = ", ".join(str(index) for index in range(1, len(columns) + 1))
        expected_sql_no_return = (
            f'INSERT INTO {table_name} '
            f'({expected_columns}) '
            f'VALUES ({expected_values});'
        )

        insert = Insert(
            table_name=table_name,
            columns=columns,
        )

        assert insert.sql_string == expected_sql_no_return

    def test_sql_string_returning_string(self):
        table_name = 'foo'
        columns = ['a', 'b', 'c']
        expected_columns = 'a, b, c'
        expected_values = ", ".join(str(index) for index in range(1, len(columns) + 1))
        returning_string = '*'
        expected_sql_return_string = (
            f'INSERT INTO {table_name} '
            f'({expected_columns}) '
            f'VALUES ({expected_values}) RETURNING {returning_string};'
        )

        insert = Insert(
            table_name=table_name,
            columns=columns,
            returning=returning_string,
        )

        assert insert.sql_string == expected_sql_return_string

    def test_sql_string_returning_list(self):
        table_name = 'foo'
        columns = ['a', 'b', 'c']
        expected_columns = 'a, b, c'
        expected_values = ", ".join(str(index) for index in range(1, len(columns) + 1))
        returning_list = ['a', 'b']
        expected_returning_list = 'a, b'
        expected_sql_return_list = (
            f'INSERT INTO {table_name} '
            f'({expected_columns}) '
            f'VALUES ({expected_values}) '
            f'RETURNING {expected_returning_list};'
        )

        insert = Insert(
            table_name=table_name,
            columns=columns,
            returning=returning_list,
        )

        assert insert.sql_string == expected_sql_return_list

