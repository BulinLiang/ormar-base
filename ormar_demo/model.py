#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid
from typing import Optional

import ormar as orm
from ormar import ReferentialAction

from ormar_demo import Base, BaseMeta


class User(Base):
    class Meta(BaseMeta):
        tablename = "users"

    name = orm.String(max_length=100, default="default_name")


class Task(Base):
    class Meta(BaseMeta):
        tablename = "tasks"

    task_id = orm.UUID(default=uuid.uuid4)
    task_name = orm.String(max_length=100)
    user: Optional[User] = orm.ForeignKey(User, ondelete=ReferentialAction.CASCADE)
