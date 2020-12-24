import asyncio
import datetime
import time

from random import choice

from tortoise import Tortoise
from tortoise.transactions import in_transaction

from models import Transaction


transaction_type_choices = ['Purchase', 'Payment', 'Credit']
transaction_type_for_search_choices = ['Purchase', 'Payment', 'Credit', 'non-exist-value']

CONCURRENTS = 10
COUNT = 1000  # ITERATIONS
COUNT = int(COUNT // CONCURRENTS) * CONCURRENTS


async def init():
    await Tortoise.init(db_url='postgres://postgres:postgres@localhost:5433/postgres', modules={'models': ['models']})
    await Tortoise.generate_schemas()


async def create_transaction(count):
    for i in range(count):
        await Transaction.create(
            amount=count,
            date_created=datetime.datetime.now(),
            transaction_type=choice(transaction_type_choices)
        )


async def get_transaction(count):
    for _ in range(count):
        await Transaction.filter(transaction_type=choice(transaction_type_for_search_choices)).first()


async def update_transaction(objs):
    async with in_transaction():
        for obj in objs:
            obj.transaction_type = choice(transaction_type_choices)
            await obj.save(update_fields=["transaction_type"])

    return len(objs)


async def delete_transaction(objs):
    async with in_transaction():
        for obj in objs:
            await obj.delete()

    return len(objs)


async def delete_all_transaction():
    await Transaction.filter().delete()


async def test_create():
    start = time.time()
    await asyncio.gather(*[create_transaction(COUNT // CONCURRENTS) for _ in range(CONCURRENTS)])
    now = time.time()
    print(f'Tortoise ORM CREATE: Rows/sec: {COUNT / (now - start): 10.2f}')
    print(f'Tortoise ORM CREATE: Execution time: {now - start: 10.2f}')


async def test_get():
    start = time.time()
    await asyncio.gather(*[get_transaction(COUNT // CONCURRENTS) for _ in range(CONCURRENTS)])
    now = time.time()
    print(f'Tortoise ORM GET: Rows/sec: {COUNT / (now - start): 10.2f}')
    print(f'Tortoise ORM GET: Execution time: {now - start: 10.2f}')


async def test_update():
    objs = list(await Transaction.all())
    inrange = len(objs) // CONCURRENTS
    if inrange < 1:
        inrange = 1

    start = time.time()
    count = sum(await asyncio.gather(
        *[update_transaction(objs[i * inrange:((i + 1) * inrange) - 1]) for i in range(CONCURRENTS)]))
    now = time.time()

    print(f'Tortoise ORM UPDATE: Rows/sec: {count / (now - start): 10.2f}')
    print(f'Tortoise ORM UPDATE: Execution time: {now - start: 10.2f}')


async def test_delete():
    objs = list(await Transaction.all())
    inrange = len(objs)
    if inrange < 1:
        inrange = 1

    start = time.time()
    result = sum(await asyncio.gather(
        *[delete_transaction(objs[i * inrange:((i + 1) * inrange) - 1]) for i in range(COUNT)]))
    now = time.time()
    print(f'Tortoise ORM DELETE: Rows/sec: {result / (now - start): 10.2f}')
    print(f'Tortoise ORM DELETE: Execution time: {now - start: 10.2f}')


async def main():
    await init()
    await delete_all_transaction()

    await test_create()
    await test_get()
    await test_update()
    await test_delete()


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    print(event_loop.run_until_complete(main()))
