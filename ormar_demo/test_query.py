#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio

from sqlalchemy import JSON, case, cast, column, delete, func, select

from ormar_demo import create_table, db, drop_table
from ormar_demo.model import Task, User

"""
ormar 没有 for update 的设置，手动更改如下：
ormar/queryset/queries/query.py 154行 ，
expr = expr.select_from(self.select_from) 改成
expr = expr.with_for_update(nowait=True, of=self.table).select_from(self.select_from)
"""
USER_NAME = "default_name"

UserORM = User.Meta.table  # noqa
TaskORM = Task.Meta.table  # noqa


async def create():
    return await User.create(), await User().save()


async def user_delete(users):
    for u in users:
        await u.delete()


async def query_exists():
    assert not await User.exists(name=USER_NAME)


async def query_use_base():
    users = await create()
    u = await User.get_by(name=USER_NAME)
    assert u.name == USER_NAME
    us = await User.get_all(name=USER_NAME)
    for u in us:
        assert u.name == USER_NAME

    await user_delete(users)


async def query_count():
    users = await create()
    amount = await User.objects.count()
    assert amount == 2
    await user_delete(users)


async def query_like():
    """https://collerek.github.io/ormar/api/queryset/#ormar.queryset.field_accessor.FieldAccessor.contains"""
    # column LIKE '%<VALUE>%'
    users = await create()
    us = await User.objects.filter(User.name.contains("ault_")).all()
    for u in us:
        assert u.name == USER_NAME
    # column LIKE '<VALUE>%'
    us = await User.objects.filter(User.name.startswith("default")).all()
    for u in us:
        assert u.name == USER_NAME
    # column LIKE '%<VALUE>'
    us = await User.objects.filter(User.name.endswith("name")).all()
    for u in us:
        assert u.name == USER_NAME
    # column LIKE '%<VALUE>%' case-insensitive
    us = await User.objects.filter(User.name.icontains("AULT_")).all()
    for u in us:
        assert u.name == USER_NAME
    # column LIKE '<VALUE>%' case-insensitive
    us = await User.objects.filter(User.name.istartswith("DEFAULT")).all()
    for u in us:
        assert u.name == USER_NAME
    # column LIKE '%<VALUE>' case-insensitive
    us = await User.objects.filter(User.name.iendswith("NAME")).all()
    for u in us:
        assert u.name == USER_NAME

    await user_delete(users)


async def query_join():
    user = await User.create(name="sqlalchemy")
    await Task.create(task_name="test_task", user=user)
    await Task.create(task_name="test_task2", user=user)

    tasks = await Task.objects.select_related(Task.user).filter(Task.user.name == "sqlalchemy").all()
    for t in tasks:
        assert t.user.name == "sqlalchemy"

    # 当有多个表进行关联查询时，使用select_all就不用像使用select_related把所有表的关联写出来
    tasks = await Task.objects.select_all(follow=True).filter(Task.task_name.contains("test_task")).all()
    for t in tasks:
        assert t.user.name == "sqlalchemy"
    # prefetch_related 和 select_related 一样，但是执行sqlalchemy语句时是一句一句执行的查询语句，效率会慢
    await user.delete()


async def query_specify_fields():
    u = await User.create(name="specify_fields")
    user = await User.objects.filter(name="specify_fields").values(["name"])
    assert user == [{"name": "specify_fields"}]
    await u.delete()


async def query_case():
    users = await User.create(name="admin"), await User.create(name=USER_NAME), await User.create(name=USER_NAME)
    user_level = select([UserORM.c.id, case((UserORM.c.name == "admin", "管理员"), else_="普通用户").label("level")]).alias(
        "_ul"
    )
    expr = select(
        [
            func.coalesce(
                func.array_to_json(func.array_agg(func.row_to_json(select([column("_ul")]).label("q")))),
                cast(list(), JSON),
            ).label("json")
        ]
    ).select_from(user_level)
    # 未格式化结果 [{"id":102,"level":"管理员"},{"id":103,"level":"普通用户"},{"id":104,"level":"普通用户"}]
    res = await db.execute(expr)
    # databases.backends.postgres.Record 对象
    res = await db.fetch_one(expr)
    # 格式化结果 [{'id': 99, 'level': '管理员'}, {'id': 100, 'level': '普通用户'}, {'id': 101, 'level': '普通用户'}]
    res = await db.fetch_val(expr)
    assert res
    await user_delete(users)


async def query_core():
    u = await User.create(name=USER_NAME)
    await Task.create(task_name="asdf", user=u)

    query = (
        select([UserORM.alias("_u"), TaskORM.c.id.label("task_id"), TaskORM.c.task_name.label("t_name")])
        .select_from(UserORM.join(TaskORM, TaskORM.c.user == UserORM.c.id))
        .where(UserORM.c.name.like("default%"))
        .order_by(UserORM.c.created_on.desc())
    )
    res = await db.fetch_one(query)

    assert res.name == USER_NAME and res.t_name == "asdf"
    await db.execute(delete(UserORM).where(UserORM.c.id == res.id))
    assert not await User.get(u.id) and not await Task.get_by(task_name="asdf")


async def main():
    await db.connect()
    await create_table()
    try:
        # await query_use_base()
        # await query_exists()
        # await query_count()
        # await query_like()
        # await query_join()
        await query_case()
        # await query_specify_fields()
        # await query_core()
    except Exception as exc:
        print("抛出异常", str(exc.__class__), exc)
    finally:
        # await drop_table()
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
