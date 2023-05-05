#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Union

import ormar as orm
import sqlalchemy as sa
from databases import Database
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("databases").setLevel(logging.DEBUG)

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:55432/sqlmodel"
db = Database(DATABASE_URL)
metadata = MetaData()
engine = AsyncEngine(create_engine(DATABASE_URL))

"""
如果使用JSONB 参考https://github.com/tophat/ormar-postgres-extensions
"""


class BaseMeta(orm.ModelMeta):
    metadata = metadata
    database = db


class Base(orm.Model):
    class Meta(BaseMeta):
        abstract = True

    id = orm.Integer(primary_key=True)
    created_on = orm.DateTime(default=datetime.now, server_default=sa.func.now(), index=True)
    updated_on = orm.DateTime(default=datetime.now, onupdate=sa.func.now(), server_default=sa.func.now(), index=True)
    version = orm.UUID(default=uuid.uuid4, nullable=False)

    def to_dict(self) -> Dict[str, Union[str, None]]:
        return {c.name: getattr(self, c.name, None) for c in self.Meta.columns}

    @classmethod
    async def create(cls, **kwargs):
        return await cls.objects.create(**kwargs)

    @classmethod
    async def first(cls):
        try:
            return await cls.objects.first()
        except orm.NoMatch:
            return None

    @classmethod
    async def get(cls, *args, **kwargs) -> Optional["Base"]:
        if args:
            kwargs.update(id=args[0])
        res = await cls.objects.filter(**kwargs).limit(limit_count=1).all()
        return res[0] if res else None

    @classmethod
    async def get_by(cls, **kwargs) -> Optional["Base"]:
        return await cls.get(**kwargs)

    @classmethod
    async def get_all(cls, **kwargs) -> List[Optional["Base"]]:
        res = await cls.objects.filter(**kwargs).all()
        return res

    @classmethod
    async def exists(cls, **kwargs) -> bool:
        return await cls.objects.filter(**kwargs).exists()


async def create_table():
    async with engine.begin() as tx:
        await tx.run_sync(metadata.create_all)


async def drop_table():
    async with engine.begin() as tx:
        await tx.run_sync(metadata.drop_all)
