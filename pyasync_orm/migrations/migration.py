import inspect
import os
from contextlib import suppress
from typing import Tuple, List, Any, TYPE_CHECKING

from pyasync_orm.orm import ORM

if TYPE_CHECKING:
    from pyasync_orm.database import Database
    from pyasync_orm.models import Model


class Migration:
    def __init__(self, database: 'Database'):
        self.database = database
        self.sql = []

    async def _get_database_data(self, table_name: str) -> Tuple[List[Any], List[Any]]:
        async with self.database.get_connection() as connection:
            column_data = await connection.fetch(self.database.management_system.column_data_sql(table_name=table_name))
            index_data = await connection.fetch(self.database.management_system.index_data_sql(table_name=table_name))
        return column_data, index_data

    async def _add_sql(self, model: 'Model'):
        column_data, index_data = await self._get_database_data(model.table_name)
        model_table = ORM.database.management_system.table_class.from_model(model_class=model)
        if column_data:
            db_table = ORM.database.management_system.table_class.from_db(
                table_name=model.table_name,
                column_data=column_data,
                index_data=index_data,
            )
            self.sql += ORM.database.management_system.get_alter_table_sql(
                model_table=model_table,
                db_table=db_table,
            )
        else:
            self.sql.append(ORM.database.management_system.get_create_table_sql(model_table=model_table))

    async def _gather_sql(self):
        for model in self.database.models:
            await self._add_sql(model=model)

    def _get_or_create_migrations_directory(self):
        migration_path = os.path.dirname(inspect.getmodule(self.database.models[0]).__file__) + '/migrations'
        with suppress(FileExistsError):
            os.mkdir(migration_path)
        os.chdir(migration_path)

    def _write_to_file(self):
        # TODO raise error if non-migration files exist
        file_number = len(os.listdir()) + 1
        with open(f'migration_{file_number}.py', 'x') as file:
            file.write('migrations = [\n')
            for sql in self.sql:
                file.write(f'\t\'{sql}\',\n')
            file.write(']\n')

    async def write_migration(self):
        await self._gather_sql()
        self._get_or_create_migrations_directory()
        self._write_to_file()
