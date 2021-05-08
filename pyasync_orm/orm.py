from typing import Optional

from pyasync_orm.sql.sql import SQL


class ORM:
    def __init__(self, model_class, _sql: Optional[SQL] = None):
        self.model_class = model_class
        self._sql = _sql
        self._values = ()

    def _get_orm(self) -> 'ORM':
        if self._sql is None:
            orm = ORM(self.model_class, SQL(self.model_class.meta.table_name))
        else:
            orm = self
        return orm

    @property
    def sql(self) -> SQL:
        return self._sql or SQL(self.model_class.meta.table_name)

    def filter(self, **kwargs) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_where(where_list=list(kwargs.keys()))
        orm._values += tuple(kwargs.values())
        return orm

    def exclude(self, **kwargs) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_where(where_list=list(kwargs.keys()), exclude=True)
        orm._values += tuple(kwargs.values())
        return orm

    def order_by(self, *args: str) -> 'ORM':
        orm = self._get_orm()
        orm._sql.add_order_by(order_by_args_length=len(args))
        orm._values += args
        return orm

    def limit(self, number: int) -> 'ORM':
        orm = self._get_orm()
        orm._sql.set_limit()
        orm._values += (number, )
        return orm

    async def count(self):
        """
        Counting rows in big tables can be slow.
        See ORM.estimate() for bigger tables (millions of rows).
        """
        sql_string = self.sql.select(columns='COUNT(*)')

    async def estimate(self):
        """
        Typically, the estimate is very close. How close, depends on whether
        ANALYZE or VACUUM are run enough - where "enough" is defined by the
        level of write activity to your table.
        https://stackoverflow.com/a/7945274
        """
        # TODO this is postgres specific
        sql_string = (
            'SELECT reltuples AS estimate FROM pg_class '
            f'where relname = \'{self.model_class.meta.table_name}\';'
        )

    async def create(self, **kwargs):
        self._values += tuple(kwargs.values())
        sql_string = self.sql.insert(columns=list(kwargs.keys()))

    async def bulk_create(self, **kwargs):
        pass

    async def get(self, **kwargs):
        self.sql.add_where(list(kwargs.keys()))
        self._values = tuple(kwargs.values())
        sql_string = self.sql.select(columns='*')

    async def all(self):
        sql_string = self.sql.select(columns='*')

    async def update(self, **kwargs):
        self._values += tuple(kwargs.values())
        sql_string = self.sql.update(set_columns=list(kwargs.keys()))

    async def bulk_update(self, **kwargs):
        pass

    async def delete(self):
        self.sql.delete()
