"""
@file: app/api/dependencies.py
@description: Зависимости для FastAPI
@dependencies: fastapi, sqlmodel
"""

from typing import AsyncGenerator, Generator
from fastapi import Depends
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.database import get_async_session, get_sync_session
from app.core.logging import get_logger
from app.core.auth import get_current_active_user, CurrentUser

logger = get_logger(__name__)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Получение асинхронной сессии базы данных для использования в эндпоинтах.
    
    Yields:
        AsyncSession: Асинхронная сессия базы данных
    """
    async for session in get_async_session():
        yield session


def get_sync_db_session() -> Generator[Session, None, None]:
    """
    Получение синхронной сессии базы данных для использования в Celery задачах и сервисах.
    
    Yields:
        Session: Синхронная сессия базы данных
    """
    for session in get_sync_session():
        yield session


def get_sync_db_session_direct() -> Session:
    """
    Прямое получение синхронной сессии базы данных для использования в сервисах.
    
    Returns:
        Session: Синхронная сессия базы данных
    """
    return next(get_sync_session())


# Типы зависимостей для использования в эндпоинтах
DatabaseSession = Depends(get_db_session)  # Для FastAPI эндпоинтов
SyncDatabaseSession = Depends(get_sync_db_session)  # Для синхронных операций
CurrentUserDep = Depends(get_current_active_user)  # Для аутентификации 