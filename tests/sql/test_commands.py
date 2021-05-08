from pyasync_orm.sql.commands import (
    InsertSQL, SelectSQL, BaseSQLCommand, UpdateSQL,
    DeleteSQL,
)


class TestBaseSQLCommand:
    def test_init(self):
        class FooCommand(BaseSQLCommand):
            @property
            def sql_string(self) -> str:
                return 'foo'

        assert FooCommand('foo').table_name == 'foo'

    def test_format_wheres_string(self):
        assert BaseSQLCommand.format_wheres_string(None) is None
        assert BaseSQLCommand.format_wheres_string(['a', 'b']) == 'a AND b'

    def test_format_returning_string(self):
        assert BaseSQLCommand.format_returning_string('*') == '*'
        assert BaseSQLCommand.format_returning_string(['a', 'b']) == 'a, b'
        assert BaseSQLCommand.format_returning_string(None) is None


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
            where=['a = 1'],
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
            where=['a = 1'],
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
            set_columns_string='a = 1',
        )

        assert update.table_name == 'foo'
        assert update.set_columns_string == 'a = 1'
        assert update.where is None
        assert update.returning is None

    def test_init_with_options(self):
        set_columns = 'a = 1'
        where = ['a = 2']

        update_returning_string = UpdateSQL(
            table_name='foo',
            set_columns_string=set_columns,
            where=where,
            returning='*'
        )
        update_returning_list = UpdateSQL(
            table_name='foo',
            set_columns_string=set_columns,
            where=where,
            returning=['a']
        )

        # update_returning_string
        assert update_returning_string.table_name == 'foo'
        assert update_returning_string.set_columns_string == 'a = 1'
        assert update_returning_string.where == 'a = 2'
        assert update_returning_string.returning == '*'
        # update_returning_list
        assert update_returning_list.table_name == 'foo'
        assert update_returning_list.set_columns_string == 'a = 1'
        assert update_returning_list.where == 'a = 2'
        assert update_returning_list.returning == 'a'

    def test_sql_string(self):
        update_sql_string = UpdateSQL(
            table_name='foo',
            set_columns_string='a = 1',
        ).sql_string

        assert update_sql_string == 'UPDATE foo SET a = 1 '

    def test_sql_string_with_optionals(self):
        update_sql_string = UpdateSQL(
            table_name='foo',
            set_columns_string='a = 1',
            where=['a = 2'],
            returning='*',
        ).sql_string

        assert update_sql_string == (
            'UPDATE foo SET a = 1 WHERE a = 2 RETURNING *'
        )


class TestDeleteSQL:
    def test_init(self):
        delete = DeleteSQL(table_name='foo')

        assert delete.table_name == 'foo'
        assert delete.where is None
        assert delete.returning is None

    def test_init_with_options(self):
        where = ['a = 2']

        delete_returning_string = DeleteSQL(
            table_name='foo',
            where=where,
            returning='*'
        )
        delete_returning_list = DeleteSQL(
            table_name='foo',
            where=where,
            returning=['a']
        )

        # delete_returning_string
        assert delete_returning_string.table_name == 'foo'
        assert delete_returning_string.where == 'a = 2'
        assert delete_returning_string.returning == '*'
        # delete_returning_list
        assert delete_returning_list.table_name == 'foo'
        assert delete_returning_list.where == 'a = 2'
        assert delete_returning_list.returning == 'a'

    def test_sql_string(self):
        delete_sql_string = DeleteSQL(table_name='foo').sql_string

        assert delete_sql_string == 'DELETE FROM foo '

    def test_sql_string_with_optionals(self):
        delete_sql_string = DeleteSQL(
            table_name='foo',
            where=['a = 2'],
            returning='*',
        ).sql_string

        assert delete_sql_string == (
            'DELETE FROM foo WHERE a = 2 RETURNING *'
        )
