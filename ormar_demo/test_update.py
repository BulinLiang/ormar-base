#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import datetime
import random
import uuid

from ormar_demo import create_table, db, drop_table
from ormar_demo.model import Task, User


async def delete(users):
    for u in users:
        await u.delete()


async def update_of_concurrent(version):
    async with db.transaction():
        user = await User.get_by(name="default_name")
        if user and user.version != version:
            await user.update(name=f"sqlalchemy_{version.hex}", version=version)
            return True
    return False


async def update_use_base():
    user = await User.create()
    await user.update(name="update_name")
    await user.delete()


async def update_raise():
    user = await User.create(name="sqlalchemy")
    try:
        async with db.transaction():
            await user.update(name="update_name")
            raise ValueError
    except:
        assert await User.get_by(name="sqlalchemy") and not await User.get_by(name="update_name")
        await user.delete()


async def query_and_update():
    await User.create(name="sqlalchemy")
    user = await User.get_by(name="sqlalchemy")
    await user.update(name="update_name", created_on=datetime.datetime.now())

    assert await User.get_by(name="update_name")
    await user.delete()


async def update_join():
    user_name = "update" + str(random.randint(1, 300))
    user = await User.create(name=user_name)
    await Task.create(task_name="test_task", user=user)
    await Task.create(task_name="test_task2", user=user)

    user = await User.objects.select_related(User.tasks).filter(name=user_name).first()
    for i, task in enumerate(user.tasks):
        await task.update(task_name=f"update_task{i}")

    tasks = await Task.objects.filter(Task.task_name.contains("update_task")).all()
    assert len(tasks) == 2

    await user.delete()


async def run_for_update_concurrent():
    user = await User.create()
    version = uuid.uuid4()
    results = await asyncio.gather(*[update_of_concurrent(version) for _ in range(30)], return_exceptions=True)
    # print(results, "\n", "=" * 50, "\n")
    assert results.count(True) == 1

    await user.delete()


async def main():
    await db.connect()
    await create_table()
    await run_for_update_concurrent()
    try:
        await update_use_base()
        await update_raise()
        await query_and_update()
        await update_join()
    except Exception as exc:
        print("抛出异常", str(exc.__class__), exc)
    finally:
        # await drop_table()
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
