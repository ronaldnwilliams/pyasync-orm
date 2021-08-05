from pyasync_orm.migrations.table import Table
from pyasync_orm.orm import ORM


class Migration:
    async def _get_database_data(self):
        async with ORM.database.get_connection() as connection:
            column_data = await connection.fetch("""
                SELECT
                    column_name, column_default, is_nullable,
                    data_type, character_maximum_length, numeric_precision,
                    numeric_scale
                FROM
                     information_schema.columns
                WHERE
                    table_name = 'customers';
            """)
            index_names = await connection.fetch("""
                SELECT
                    indexname
                FROM
                    pg_indexes
                WHERE
                    tablename = 'customers';
            """)
        return column_data, index_names

    async def create(self):
        column_data, index_names = await self._get_database_data()
        db_table = Table.from_db(table_name='customers', column_data=column_data, index_names=index_names)
        # model_table = Table.from_model()
