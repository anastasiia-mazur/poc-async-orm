import asyncio
import peewee
import peewee_async
import uuid
import datetime
import time
from random import choice

CONCURRENTS = 10
COUNT = 1000  # ITERATIONS
COUNT = int(COUNT // CONCURRENTS) * CONCURRENTS

transaction_type_choices = ['Purchase', 'Payment', 'Credit']
transaction_type_for_search_choices = ['Purchase', 'Payment', 'Credit', 'non-exist-value']


database = peewee_async.PostgresqlDatabase(
    database='postgres',
    user='postgres',
    host='127.0.0.1',
    port='5433',
    password='postgres'
)


class Transaction(peewee.Model):
    id = peewee.UUIDField(unique=True, primary_key=True)
    amount = peewee.DecimalField(max_digits=10, decimal_places=2)
    date_created = peewee.DateTimeField()
    transaction_type = peewee.TextField()

    class Meta:
        database = database

# sync code is working!
Transaction.create_table(True)

# Create async models manager:
objects = peewee_async.Manager(database)

# No need for sync anymore!
database.set_allow_sync(False)


async def create_transaction(count):
    for i in range(count):
        await objects.create(
            Transaction,
            id=str(uuid.uuid4()),
            amount=count,
            date_created=datetime.datetime.now(),
            transaction_type=choice(transaction_type_choices)
        )


async def get_transaction(count):
    for _ in range(count):
        try:
            await objects.get(Transaction, transaction_type=choice(transaction_type_for_search_choices))
        except peewee.DoesNotExist:
            pass


async def update_transaction(objs):
    for obj in objs:
        obj.transaction_type = choice(transaction_type_choices)
        await objects.update(obj)
    return len(objs)


async def delete_transaction(objs):
    for obj in objs:
        await objects.delete(obj)

    return len(objs)


async def test_create():
    start = time.time()
    await asyncio.gather(*[create_transaction(COUNT // CONCURRENTS) for _ in range(CONCURRENTS)])
    now = time.time()
    print(f'PEEWEE ASYNC CREATE: Rows/sec: {COUNT / (now - start): 10.2f}')
    print(f'PEEWEE ASYNC CREATE: Execution time: {now - start: 10.2f}')


async def test_get():
    start = time.time()
    await asyncio.gather(*[get_transaction(COUNT // CONCURRENTS) for _ in range(CONCURRENTS)])
    now = time.time()
    print(f'PEEWEE ASYNC GET: Rows/sec: {COUNT / (now - start): 10.2f}')
    print(f'PEEWEE ASYNC GET: Execution time: {now - start: 10.2f}')


async def test_update():
    objs = list(await objects.execute(Transaction.select()))
    inrange = len(objs) // CONCURRENTS
    if inrange < 1:
        inrange = 1

    start = time.time()
    count = sum(await asyncio.gather(
        *[update_transaction(objs[i * inrange:((i + 1) * inrange) - 1]) for i in range(CONCURRENTS)]))
    now = time.time()
    print(f'PEEWEE ASYNC UPDATE: Rows/sec: {count / (now - start): 10.2f}')
    print(f'PEEWEE ASYNC UPDATE: Execution time: {now - start: 10.2f}')


async def test_delete():
    objs = list(await objects.execute(Transaction.select()))
    inrange = len(objs)
    if inrange < 1:
        inrange = 1

    start = time.time()
    result = sum(await asyncio.gather(
        *[delete_transaction(objs[i * inrange:((i + 1) * inrange) - 1]) for i in range(COUNT)]))
    now = time.time()
    print(f'PEEWEE ASYNC DELETE: Rows/sec: {result / (now - start): 10.2f}')
    print(f'PEEWEE ASYNC DELETE: Execution time: {now - start: 10.2f}')


async def main():
    await test_create()
    await test_get()
    await test_update()
    await test_delete()
    database.close()
    with objects.allow_sync():
        Transaction.drop_table(True)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

