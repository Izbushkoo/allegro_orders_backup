"""
@file: app/api/dependencies.py
@description: Зависимости для FastAPI
@dependencies: fastapi, sqlmodel
"""

from typing import AsyncGenerator
from fastapi import Depends
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.database import get_async_session
from app.core.logging import get_logger

logger = get_logger(__name__)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Получение сессии базы данных для использования в эндпоинтах.
    
    Yields:
        AsyncSession: Асинхронная сессия базы данных
    """
    async for session in get_async_session():
        yield session


# Типы зависимостей для использования в эндпоинтах
DatabaseSession = Depends(get_db_session) 