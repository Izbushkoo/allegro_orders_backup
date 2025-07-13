"""
@file: app/core/logging.py
@description: Настройка системы логирования с JSON форматом и ротацией
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
    
    # Форматировщик
    if settings.logging.format.lower() == "json":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    
    # Handler для файла с ротацией
    file_handler = logging.handlers.RotatingFileHandler(
        filename=settings.logging.file_path,
        maxBytes=settings.logging.max_bytes,
        backupCount=settings.logging.backup_count,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler для консоли (только в режиме разработки)
    if settings.api.debug:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Настройка уровней для внешних библиотек
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    logging.getLogger("celery").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    
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