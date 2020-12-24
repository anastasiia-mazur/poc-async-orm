import uuid
import asyncio
import time

from gino import Gino
import datetime
from random import choice


CONCURRENTS = 10
COUNT = 1000  # ITERATIONS
COUNT = int(COUNT // CONCURRENTS) * CONCURRENTS

transaction_type_choices = ['Purchase', 'Payment', 'Credit']
transaction_type_for_search_choices = ['Purchase', 'Payment', 'Credit', 'non-exist-value']


db = Gino()


class Transaction(db.Model):
    __tablename__ = 'transaction'

    id = db.Column(db.Unicode, unique=True)
    amount = db.Column(db.Numeric((10, 2)))
    date_created = db.Column(db.DateTime)
    transaction_type = db.Column(db.Unicode)


async def create_transaction(count):
    for i in range(count):
        await Transaction.create(
            id=str(uuid.uuid4()),
            amount=count,
            date_created=datetime.datetime.now(),
            transaction_type=choice(transaction_type_choices)
        )


async def get_transaction(count):
    for _ in range(count):
        await Transaction.query.where(
            Transaction.transaction_type == choice(transaction_type_for_search_choices)
        ).gino.first()


async def update_transaction(objs):
    for obj in objs:
        await Transaction.update.values(transaction_type=choice(transaction_type_choices)).where(
            Transaction.id == obj.id).gino.status()
    return len(objs)


async def delete_transaction(objs):
    for obj in objs:
        await Transaction.delete.where(Transaction.id > obj.id).gino.status()

    return len(objs)


async def test_create():
    start = time.time()
    await asyncio.gather(*[create_transaction(COUNT // CONCURRENTS) for _ in range(CONCURRENTS)])
    now = time.time()
    print(f'GINO ORM CREATE: Rows/sec: {COUNT / (now - start): 10.2f}')
    print(f'GINO ORM CREATE: Execution time: {now - start: 10.2f}')


async def test_get():
    start = time.time()
    await asyncio.gather(*[get_transaction(COUNT // CONCURRENTS) for _ in range(CONCURRENTS)])
    now = time.time()
    print(f'GINO ORM GET: Rows/sec: {COUNT / (now - start): 10.2f}')
    print(f'GINO ORM GET: Execution time: {now - start: 10.2f}')


async def test_update():
    objs = list(await Transaction.query.gino.all())
    inrange = len(objs) // CONCURRENTS
    if inrange < 1:
        inrange = 1

    start = time.time()
    count = sum(await asyncio.gather(
        *[update_transaction(objs[i * inrange:((i + 1) * inrange) - 1]) for i in range(CONCURRENTS)]))
    now = time.time()
    print(f'GINO ORM UPDATE: Rows/sec: {count / (now - start): 10.2f}')
    print(f'GINO ORM UPDATE: Execution time: {now - start: 10.2f}')


async def test_delete():
    objs = list(await Transaction.query.gino.all())
    inrange = len(objs)
    if inrange < 1:
        inrange = 1

    start = time.time()
    result = sum(await asyncio.gather(
        *[delete_transaction(objs[i * inrange:((i + 1) * inrange) - 1]) for i in range(COUNT)]))
    now = time.time()
    print(f'GINO ORM DELETE: Rows/sec: {result / (now - start): 10.2f}')
    print(f'GINO ORM DELETE: Execution time: {now - start: 10.2f}')


async def main():
    await db.set_bind('postgresql://postgres:postgres@localhost:5433/postgres')
    await db.gino.create_all()
    await test_create()
    await test_get()
    await test_update()
    await test_delete()
    await db.pop_bind().close()


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    print(event_loop.run_until_complete(main()))
