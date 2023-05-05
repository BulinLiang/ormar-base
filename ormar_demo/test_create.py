#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio

from ormar_demo import create_table, db, drop_table
from ormar_demo.model import Task, User


async def create_use_base():
    task = await Task.get_by(task_name="test_task")
    user = await User.get_by(name="sqlalchemy")
    if not user:
        user = await User.create(name="sqlalchemy")
    if not task:
        await Task.create(task_name="test_task", user=user)
        await Task.create(task_name="test_task2", user=user)
    assert await Task.objects.count(Task.task_name.startswith("test_task")) == 2
    assert await User.objects.count(User.name == "sqlalchemy") == 1

    await user.delete()
    await Task.objects.delete(Task.task_name.startswith("test_task"))


async def create_default():
    user1 = await User.create()
    user2 = await User().save()
    assert user1.name == "default_name" and user2.name == "default_name"
    await user1.delete()
    await user2.delete()


async def create_raise():
    try:
        async with db.transaction():
            await User.create(name="aaaa")
            raise ValueError
    except:
        assert not await User.get(name="aaaa")


async def main():
    await db.connect()
    await create_table()
    try:
        await create_use_base()
        await create_default()
        await create_raise()
    except Exception as exc:
        print("抛出异常", str(exc.__class__), exc)
    finally:
        # await drop_table()
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
