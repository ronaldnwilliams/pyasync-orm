from pyasync_orm.orm import ORM


class Migration:
    async def _get_database_data(self, table_name: str):
        async with ORM.database.get_connection() as connection:
            column_data = await connection.fetch(ORM.database.management_system.column_data_sql)
            index_names = await connection.fetch(ORM.database.management_system.index_names_sql)
        return column_data, index_names

    async def create(self):
        for model in ORM.database.models:
            column_data, index_names = await self._get_database_data(model.table_name)
            db_table = ORM.database.management_system.table_class.from_db(
                table_name=model.table_name,
                column_data=column_data,
                index_names=index_names,
            )
            model_table = ORM.database.management_system.table_class.from_model(model_class=model)
