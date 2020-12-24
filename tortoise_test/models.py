from tortoise import fields
from tortoise.models import Model


class Transaction(Model):
    id = fields.UUIDField(pk=True)
    amount = fields.DecimalField(max_digits=10, decimal_places=2)
    date_created = fields.DatetimeField()
    transaction_type = fields.TextField()
