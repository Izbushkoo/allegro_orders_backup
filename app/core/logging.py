"""
@file: app/core/logging.py
@description: Настройка системы логирования с удобочитаемым форматом и ротацией
@dependencies: logging, json
"""

import json
import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .settings import settings


def disable_sqlalchemy_logging():
    """Принудительно отключает все SQLAlchemy логи"""
    sqlalchemy_loggers = [
        "sqlalchemy",
        "sqlalchemy.engine",
        "sqlalchemy.engine.Engine", 
        "sqlalchemy.engine.base.Engine",
        "sqlalchemy.engine.base",
        "sqlalchemy.pool",
        "sqlalchemy.dialects",
        "sqlalchemy.orm",
    ]
    
    for logger_name in sqlalchemy_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.CRITICAL + 1)  # Выше CRITICAL
        logger.disabled = True  # Полностью отключаем логгер
        logger.propagate = False  # Не передаем в родительские логгеры
        # Убираем все handlers
        logger.handlers.clear()


class SQLAlchemyFilter(logging.Filter):
    """Фильтр для блокирования всех SQLAlchemy логов"""
    
    def filter(self, record):
        # Блокируем все логи от SQLAlchemy
        if record.name.startswith('sqlalchemy'):
            return False
        return True


class HumanReadableFormatter(logging.Formatter):
    """Форматировщик для удобочитаемых логов"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Форматирует запись в удобочитаемом формате"""
        # Базовое время и уровень
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        level_emoji = self._get_level_emoji(record.levelno)
        level_name = record.levelname.ljust(8)
        
        # Основное сообщение
        message = record.getMessage()
        
        # Дополнительная информация
        extra_info = []
        
        # Добавляем user_id если есть
        if hasattr(record, "user_id"):
            extra_info.append(f"user={record.user_id}")
            
        # Добавляем task_id если есть
        if hasattr(record, "task_id"):
            extra_info.append(f"task={record.task_id}")
            
        # Добавляем request_id если есть
        if hasattr(record, "request_id"):
            extra_info.append(f"req={record.request_id}")
            
        # Добавляем module если это не основной логгер
        if record.name != "root" and record.name != "__main__":
            extra_info.append(f"module={record.name}")
            
        # Формируем строку
        result = f"{timestamp} {level_emoji} {level_name} {message}"
        
        if extra_info:
            result += f" | {' | '.join(extra_info)}"
            
        # Добавляем исключение если есть
        if record.exc_info:
            result += f"\n{self.formatException(record.exc_info)}"
            
        return result
    
    def _get_level_emoji(self, levelno: int) -> str:
        """Возвращает эмодзи для уровня логирования"""
        if levelno >= logging.CRITICAL:
            return "🚨"
        elif levelno >= logging.ERROR:
            return "❌"
        elif levelno >= logging.WARNING:
            return "⚠️"
        elif levelno >= logging.INFO:
            return "ℹ️"
        else:
            return "🔍"


class JSONFormatter(logging.Formatter):
    """Форматировщик для JSON логов"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Форматирует запись в JSON"""
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Добавляем информацию об исключении если есть
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Добавляем дополнительные поля если есть
        if hasattr(record, "extra_data"):
            log_entry["extra"] = record.extra_data
            
        # Добавляем user_id и request_id если есть
        if hasattr(record, "user_id"):
            log_entry["user_id"] = record.user_id
        if hasattr(record, "request_id"):
            log_entry["request_id"] = record.request_id
        if hasattr(record, "task_id"):
            log_entry["task_id"] = record.task_id
            
        return json.dumps(log_entry, ensure_ascii=False)


def setup_logging() -> None:
    """Настройка системы логирования"""
    
    # Отключаем SQLAlchemy логи в самом начале
    disable_sqlalchemy_logging()
    
    # Создаем директорию для логов
    log_path = Path(settings.logging.file_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Базовая конфигурация
    logging.basicConfig(
        level=getattr(logging, settings.logging.level.upper()),
        format="%(message)s",
        handlers=[]
    )
    
    # Создаем основной логгер
    logger = logging.getLogger()
    logger.handlers.clear()
    
    # Создаем фильтр для блокирования SQLAlchemy
    sqlalchemy_filter = SQLAlchemyFilter()
    
    # Выбираем форматировщик
    if settings.logging.format.lower() == "json":
        formatter = JSONFormatter()
    else:
        formatter = HumanReadableFormatter()
    
    # Handler для файла с ротацией
    file_handler = logging.handlers.RotatingFileHandler(
        filename=settings.logging.file_path,
        maxBytes=settings.logging.max_bytes,
        backupCount=settings.logging.backup_count,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(sqlalchemy_filter)  # Добавляем фильтр
    logger.addHandler(file_handler)
    
    # Handler для консоли (только в режиме разработки)
    if settings.api.debug:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.addFilter(sqlalchemy_filter)  # Добавляем фильтр
        logger.addHandler(console_handler)
    
    # Настройка уровней для внешних библиотек
    # SQLAlchemy уже отключен выше
    
    # Celery - только важные сообщения
    logging.getLogger("celery").setLevel(logging.WARNING)
    logging.getLogger("celery.worker").setLevel(logging.INFO)
    logging.getLogger("celery.task").setLevel(logging.INFO)
    logging.getLogger("celery.worker.strategy").setLevel(logging.WARNING)
    
    # FastAPI и Uvicorn
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.WARNING)
    
    # HTTP клиенты
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    # Redis
    logging.getLogger("redis").setLevel(logging.WARNING)
    
    # Alembic
    logging.getLogger("alembic").setLevel(logging.WARNING)
    
    # Другие библиотеки
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    
    # Еще раз отключаем SQLAlchemy логи для гарантии
    disable_sqlalchemy_logging()
    
    logger.info("Logging system initialized", extra={
        "extra_data": {
            "log_level": settings.logging.level,
            "log_file": settings.logging.file_path,
            "format": settings.logging.format
        }
    })


def get_logger(name: str) -> logging.Logger:
    """Получить логгер с настроенным именем"""
    return logging.getLogger(name)


class LoggerMixin:
    """Миксин для добавления логгера к классам"""
    
    @property
    def logger(self) -> logging.Logger:
        """Логгер с именем класса"""
        return get_logger(self.__class__.__name__)


def log_request(request_id: str, user_id: str = None):
    """Декоратор для логирования HTTP запросов"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__name__)
            
            # Логируем начало запроса
            logger.info(
                f"Request started: {func.__name__}",
                extra={
                    "request_id": request_id,
                    "user_id": user_id,
                    "extra_data": {"args": str(args), "kwargs": str(kwargs)}
                }
            )
            
            try:
                result = func(*args, **kwargs)
                logger.info(
                    f"Request completed: {func.__name__}",
                    extra={"request_id": request_id, "user_id": user_id}
                )
                return result
            except Exception as e:
                logger.error(
                    f"Request failed: {func.__name__}",
                    extra={
                        "request_id": request_id,
                        "user_id": user_id,
                        "extra_data": {"error": str(e)}
                    },
                    exc_info=True
                )
                raise
        return wrapper
    return decorator


def log_celery_task(task_id: str):
    """Декоратор для логирования Celery задач"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__name__)
            
            # Логируем начало задачи
            logger.info(
                f"Celery task started: {func.__name__}",
                extra={
                    "task_id": task_id,
                    "extra_data": {"args": str(args), "kwargs": str(kwargs)}
                }
            )
            
            try:
                result = func(*args, **kwargs)
                logger.info(
                    f"Celery task completed: {func.__name__}",
                    extra={"task_id": task_id}
                )
                return result
            except Exception as e:
                logger.error(
                    f"Celery task failed: {func.__name__}",
                    extra={
                        "task_id": task_id,
                        "extra_data": {"error": str(e)}
                    },
                    exc_info=True
                )
                raise
        return wrapper
    return decorator 