"""
@file: conftest.py
@description: Фикстуры для интеграционных тестов (асинхронная сессия БД)
@dependencies: pytest, SQLModel, app.core.database
"""

import pytest
from sqlmodel.ext.asyncio.session import AsyncSession
from app.core.database import async_session_factory

@pytest.fixture()
async def async_session():
    """
    Асинхронная сессия для интеграционных тестов с автоматическим rollback.
    Использует тестовую БД, изолирует изменения.
    """
    async with async_session_factory() as session:
        trans = await session.begin()
        try:
            yield session
        finally:
            await trans.rollback()
            await session.close() 