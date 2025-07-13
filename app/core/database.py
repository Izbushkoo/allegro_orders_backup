"""
@file: app/core/database.py
@description: Настройка подключения к базе данных PostgreSQL через SQLModel
@dependencies: sqlmodel, psycopg2
"""

import asyncio
from typing import AsyncGenerator
from sqlmodel import SQLModel, create_engine, Session
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from .settings import settings
from .logging import get_logger

logger = get_logger(__name__)

# Синхронный движок для Alembic миграций
sync_engine = create_engine(
    settings.database.url,
    echo=settings.api.debug,
    pool_pre_ping=True,
    pool_recycle=300,
)

# Асинхронный движок для FastAPI
async_database_url = settings.database.url.replace("postgresql://", "postgresql+asyncpg://")
async_engine = create_async_engine(
    async_database_url,
    echo=settings.api.debug,
    pool_pre_ping=True,
    pool_recycle=300,
)

# Фабрика сессий
async_session_factory = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


def create_tables() -> None:
    """
    Создание всех таблиц в базе данных.
    Используется только для начальной инициализации.
    Для продакшена используйте Alembic миграции.
    """
    logger.info("Creating database tables...")
    SQLModel.metadata.create_all(sync_engine)
    logger.info("Database tables created successfully")


def get_sync_session() -> Session:
    """Получить синхронную сессию базы данных для Celery и миграций"""
    with Session(sync_engine) as session:
        try:
            yield session
        except Exception as e:
            logger.error(f"Database session error: {e}")
            session.rollback()
            raise
        finally:
            session.close()


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Получить асинхронную сессию базы данных для FastAPI"""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception as e:
            logger.error(f"Database session error: {e}")
            await session.rollback()
            raise
        finally:
            await session.close()


async def check_database_connection() -> bool:
    """Проверка подключения к базе данных"""
    try:
        async with async_session_factory() as session:
            result = await session.execute(text("SELECT 1"))
            await session.commit()
            logger.info("Database connection successful")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


class DatabaseManager:
    """Менеджер базы данных для управления подключениями"""
    
    def __init__(self):
        self.sync_engine = sync_engine
        self.async_engine = async_engine
        self.logger = get_logger(self.__class__.__name__)
    
    async def startup(self) -> None:
        """Инициализация при запуске приложения"""
        self.logger.info("Initializing database connection...")
        
        # Проверяем подключение с retry логикой
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            self.logger.info(f"Database connection attempt {attempt + 1}/{max_retries}")
            is_connected = await check_database_connection()
            
            if is_connected:
                self.logger.info("Database manager initialized successfully")
                return
                
            if attempt < max_retries - 1:
                self.logger.warning(f"Database connection failed, retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 10)  # Exponential backoff
        
        self.logger.error("Failed to connect to database after all retries")
        raise Exception("Failed to connect to database after all retries")
    
    async def shutdown(self) -> None:
        """Закрытие соединений при остановке приложения"""
        self.logger.info("Closing database connections...")
        await self.async_engine.dispose()
        self.logger.info("Database connections closed")


# Глобальный экземпляр менеджера базы данных
db_manager = DatabaseManager() 