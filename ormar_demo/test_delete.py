#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

from ormar_demo import create_table, db, drop_table
from ormar_demo.model import Task, User


async def create():
    return await User.create(), await User().save()


async def delete(users):
    for u in users:
        await u.delete()


async def delete_use_base():
    users = await create()
    await delete(users)
    users = await User.get_all(name="default_name")

    assert not users


async def delete_raise():
    user = await User.create(name="sqlalchemy")
    try:
        async with db.transaction():
            await user.delete()
            raise ValueError
    except:
        assert await User.get_by(name="sqlalchemy")
        await user.delete()


async def main():
    await db.connect()
    await create_table()
    try:
        await delete_use_base()
        await delete_raise()
    except Exception as exc:
        print("抛出异常", str(exc.__class__), exc)
    finally:
        # await drop_table()
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
