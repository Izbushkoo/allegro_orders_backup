"""
@file: app/main.py
@description: Главное FastAPI приложение
@dependencies: fastapi, uvicorn
"""

from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.settings import settings
from app.core.logging import setup_logging, get_logger
from app.core.database import db_manager
from app.core.auth import CurrentUser
from app.api.dependencies import CurrentUserDep


logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    logger.info("Starting Allegro Orders Backup service...")
    
    # Инициализация логирования
    setup_logging()
    
    # Логирование настроек приложения
    logger.info("Logging application configuration...")
    config_info = settings.log_configuration()
    
    # Логируем переменные окружения
    logger.info("Environment variables status:", extra={
        "environment_variables": config_info["environment_variables"]
    })
    
    # Логируем основные настройки (без чувствительных данных)
    logger.info("Application configuration loaded:", extra={
        "database": config_info["database"],
        "redis": config_info["redis"], 
        "api": config_info["api"],
        "celery": config_info["celery"],
        "allegro": config_info["allegro"],
        "logging": config_info["logging"]
    })
    
    # Инициализация базы данных
    await db_manager.startup()
    
    logger.info("Service started successfully")
    
    yield
    
    # Остановка
    logger.info("Shutting down service...")
    await db_manager.shutdown()
    logger.info("Service stopped")


# Создание FastAPI приложения
app = FastAPI(
    title="Allegro Orders Backup",
    description="""
    ## Микросервис для резервного копирования заказов из Allegro API
    
    Этот сервис предоставляет REST API для:
    * **Управления токенами** - создание, обновление и управление токенами Allegro
    * **Синхронизации заказов** - автоматическое и ручное получение заказов из Allegro
    * **Просмотра данных** - получение информации о заказах и истории синхронизации
    * **Мониторинга** - отслеживание статуса синхронизации и статистики
    
    ### Основные возможности:
    - 🔐 Авторизация через Device Code Flow
    - 📦 Автоматическая синхронизация заказов
    - 📊 Детальная статистика и мониторинг
    - 🔄 Event-driven архитектура
    - 📋 Полная история операций
    
    ### Технологии:
    - FastAPI + SQLModel + PostgreSQL
    - Celery + Redis для фоновых задач
    - Docker для контейнеризации
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url=f"{settings.api.prefix}/openapi.json",
    lifespan=lifespan,
    contact={
        "name": "Allegro Orders Backup API",
        "url": "https://github.com/yourusername/allegro-orders-backup",
        "email": "admin@example.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    openapi_tags=[
        {
            "name": "general",
            "description": "Общие операции: health check, конфигурация",
        },
        {
            "name": "tokens",
            "description": "Управление токенами Allegro: создание, обновление, удаление",
        },
        {
            "name": "orders",
            "description": "Операции с заказами: просмотр, статистика, события",
        },
        {
            "name": "sync",
            "description": "Синхронизация заказов: запуск, мониторинг, история",
        },
    ]
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене настроить конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Базовые routes
@app.get("/", tags=["general"], summary="Главная страница API")
async def root():
    """
    Базовый endpoint с информацией о сервисе.
    
    Возвращает основную информацию о микросервисе.
    """
    return {
        "service": "Allegro Orders Backup",
        "version": "1.0.0",
        "status": "running",
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "openapi_url": f"{settings.api.prefix}/openapi.json"
    }


@app.get("/health", tags=["general"], summary="Проверка состояния сервиса")
async def health_check():
    """
    Health check endpoint для мониторинга.
    
    Проверяет:
    - Доступность базы данных
    - Общее состояние сервиса
    - Версию приложения
    """
    try:
        # Проверяем подключение к базе данных
        from app.core.database import check_database_connection
        db_status = await check_database_connection()
        
        return {
            "status": "healthy" if db_status else "unhealthy",
            "database": "connected" if db_status else "disconnected",
            "service": "Allegro Orders Backup",
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e),
            "service": "Allegro Orders Backup",
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat()
        }


@app.get("/config", tags=["general"], summary="Конфигурация приложения")
async def get_configuration():
    """
    Просмотр текущей конфигурации приложения.
    
    **Внимание:** Чувствительные данные маскируются.
    Используется для отладки и мониторинга.
    """
    try:
        config_info = settings.log_configuration()
        return {
            "service": "Allegro Orders Backup",
            "version": "1.0.0",
            "configuration": config_info,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get configuration: {e}")
        return {
            "error": "Failed to load configuration",
            "details": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/test-jwt", tags=["test"], summary="Тестирование JWT токена")
async def test_jwt_token(current_user: CurrentUser = CurrentUserDep):
    """
    Тестовый эндпоинт для проверки JWT аутентификации.
    
    **Требует аутентификации через JWT токен.**
    
    Возвращает информацию о текущем пользователе извлеченную из JWT токена.
    """
    return {
        "message": "JWT токен успешно декодирован",
        "current_user": {
            "user_id": current_user.user_id,
            "username": current_user.username,
            "is_active": current_user.is_active
        },
        "timestamp": datetime.now().isoformat()
    }


@app.post("/test-jwt/create", tags=["test"], summary="Создание тестового JWT токена")
async def create_test_jwt_token(user_id: str = "test_user_123", username: str = "test_user"):
    """
    Создает тестовый JWT токен для проверки аутентификации.
    
    **ТОЛЬКО ДЛЯ ТЕСТИРОВАНИЯ!**
    
    - **user_id**: ID пользователя для токена
    - **username**: Имя пользователя для токена
    """
    from app.core.auth import create_access_token
    
    token_data = {
        "user_id": user_id,
        "username": username
    }
    
    access_token = create_access_token(data=token_data)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user_id": user_id,
        "username": username,
        "instructions": "Используйте этот токен в заголовке: Authorization: Bearer <access_token>"
    }


# Подключение роутеров
from app.api.v1.api import api_router
app.include_router(api_router, prefix=settings.api.prefix)


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.api.host,
        port=settings.api.port,
        reload=settings.api.debug,
        log_level=settings.logging.level.lower()
    ) 