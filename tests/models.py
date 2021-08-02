from pyasync_orm import fields
from pyasync_orm.models import Model


class Customer(Model):
    first_name = fields.VarCharField(max_length=100)
