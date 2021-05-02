from pyasync_orm.sql.base import Insert


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

