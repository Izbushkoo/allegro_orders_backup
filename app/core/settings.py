"""
@file: app/core/settings.py
@description: Конфигурация приложения с использованием Pydantic Settings
@dependencies: pydantic, python-dotenv
"""

import os
from typing import Optional, Dict, Any
from pydantic import Field
from pydantic_settings import BaseSettings


def mask_sensitive_value(key: str, value: str) -> str:
    """Маскирует чувствительные данные для логирования"""
    sensitive_keys = ['password', 'secret', 'key', 'token', 'client_secret']
    
    if any(sensitive in key.lower() for sensitive in sensitive_keys):
        if len(value) <= 4:
            return '*' * len(value)
        return f"{value[:2]}{'*' * (len(value) - 4)}{value[-2:]}"
    return value


def log_environment_variables() -> Dict[str, str]:
    """Возвращает словарь переменных окружения для логирования"""
    env_vars = {}
    
    # Список всех переменных, которые используются в настройках
    expected_vars = [
        'DATABASE_URL', 'DATABASE_HOST', 'DATABASE_PORT', 'DATABASE_NAME', 
        'DATABASE_USER', 'DATABASE_PASSWORD',
        'REDIS_URL', 'REDIS_HOST', 'REDIS_PORT', 'REDIS_DB',
        'API_HOST', 'API_PORT', 'DEBUG', 'API_PREFIX', 'SECRET_KEY',
        'API_KEY_HEADER', 'TOKEN_EXPIRE_HOURS',
        'CELERY_BROKER_URL', 'CELERY_RESULT_BACKEND', 'CELERY_TASK_SERIALIZER',
        'CELERY_RESULT_SERIALIZER', 'CELERY_TIMEZONE',
        'ALLEGRO_CLIENT_ID', 'ALLEGRO_CLIENT_SECRET', 'ALLEGRO_API_URL',
        'ALLEGRO_AUTH_URL', 'ALLEGRO_SANDBOX_MODE',
        'ALLEGRO_RATE_LIMIT_GENERAL', 'ALLEGRO_RATE_LIMIT_ORDERS',
        'ALLEGRO_RATE_LIMIT_EVENTS', 'ALLEGRO_RATE_LIMIT_AUTH',
        'LOG_LEVEL', 'LOG_FILE_PATH', 'LOG_MAX_BYTES', 'LOG_BACKUP_COUNT', 'LOG_FORMAT',
        'DEFAULT_SYNC_INTERVAL_HOURS', 'ORDER_EVENTS_CHECK_INTERVAL_MINUTES',
        'TOKEN_REFRESH_INTERVAL_MINUTES', 'CLEANUP_INTERVAL_DAYS'
    ]
    
    for var in expected_vars:
        value = os.getenv(var)
        if value is not None:
            env_vars[var] = mask_sensitive_value(var, value)
        else:
            env_vars[var] = '[DEFAULT]'
    
    return env_vars


class DatabaseSettings(BaseSettings):
    """Настройки базы данных"""
    
    url: str = Field(
        default="postgresql://allegro_user:allegro_password@postgres:5432/allegro_orders", 
        alias="DATABASE_URL"
    )
    host: str = Field(default="postgres", alias="DATABASE_HOST")
    port: int = Field(default=5432, alias="DATABASE_PORT")
    name: str = Field(default="allegro_orders", alias="DATABASE_NAME")
    user: str = Field(default="allegro_user", alias="DATABASE_USER")
    password: str = Field(default="allegro_password", alias="DATABASE_PASSWORD")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class RedisSettings(BaseSettings):
    """Настройки Redis"""
    
    url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")
    host: str = Field(default="redis", alias="REDIS_HOST")
    port: int = Field(default=6379, alias="REDIS_PORT")
    db: int = Field(default=0, alias="REDIS_DB")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class APISettings(BaseSettings):
    """Настройки FastAPI"""
    
    host: str = Field(default="0.0.0.0", alias="API_HOST")
    port: int = Field(default=8000, alias="API_PORT")
    debug: bool = Field(default=False, alias="DEBUG")
    prefix: str = Field(default="/api/v1", alias="API_PREFIX")
    secret_key: str = Field(default="dev_secret_key_change_in_production", alias="SECRET_KEY")
    api_key_header: str = Field(default="X-API-Key", alias="API_KEY_HEADER")
    token_expire_hours: int = Field(default=24, alias="TOKEN_EXPIRE_HOURS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class CelerySettings(BaseSettings):
    """Настройки Celery"""
    
    broker_url: str = Field(default="redis://redis:6379/0", alias="CELERY_BROKER_URL")
    result_backend: str = Field(default="redis://redis:6379/0", alias="CELERY_RESULT_BACKEND")
    task_serializer: str = Field(default="json", alias="CELERY_TASK_SERIALIZER")
    result_serializer: str = Field(default="json", alias="CELERY_RESULT_SERIALIZER")
    accept_content: list[str] = Field(default=["json"])
    timezone: str = Field(default="UTC", alias="CELERY_TIMEZONE")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class AllegroSettings(BaseSettings):
    """Настройки Allegro API"""
    
    client_id: str = Field(default="your_allegro_client_id", alias="ALLEGRO_CLIENT_ID")
    client_secret: str = Field(default="your_allegro_client_secret", alias="ALLEGRO_CLIENT_SECRET")
    api_url: str = Field(default="https://api.allegro.pl", alias="ALLEGRO_API_URL")
    auth_url: str = Field(default="https://allegro.pl/auth/oauth", alias="ALLEGRO_AUTH_URL")
    sandbox_mode: bool = Field(default=False, alias="ALLEGRO_SANDBOX_MODE")
    
    # Rate limits
    rate_limit_general: int = Field(default=1000, alias="ALLEGRO_RATE_LIMIT_GENERAL")
    rate_limit_orders: int = Field(default=100, alias="ALLEGRO_RATE_LIMIT_ORDERS")
    rate_limit_events: int = Field(default=60, alias="ALLEGRO_RATE_LIMIT_EVENTS")
    rate_limit_auth: int = Field(default=10, alias="ALLEGRO_RATE_LIMIT_AUTH")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class LoggingSettings(BaseSettings):
    """Настройки логирования"""
    
    level: str = Field(default="INFO", alias="LOG_LEVEL")
    file_path: str = Field(default="./logs/app.log", alias="LOG_FILE_PATH")
    max_bytes: int = Field(default=5242880, alias="LOG_MAX_BYTES")  # 5MB
    backup_count: int = Field(default=3, alias="LOG_BACKUP_COUNT")
    format: str = Field(default="json", alias="LOG_FORMAT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class SyncSettings(BaseSettings):
    """Настройки синхронизации"""
    
    default_interval_hours: int = Field(default=6, alias="DEFAULT_SYNC_INTERVAL_HOURS")
    order_events_check_interval_minutes: int = Field(
        default=3, alias="ORDER_EVENTS_CHECK_INTERVAL_MINUTES"
    )
    token_refresh_interval_minutes: int = Field(
        default=30, alias="TOKEN_REFRESH_INTERVAL_MINUTES"
    )
    cleanup_interval_days: int = Field(default=1, alias="CLEANUP_INTERVAL_DAYS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class Settings(BaseSettings):
    """Основные настройки приложения"""
    
    # Подключение всех настроек
    database: DatabaseSettings = DatabaseSettings()
    redis: RedisSettings = RedisSettings()
    api: APISettings = APISettings()
    celery: CelerySettings = CelerySettings()
    allegro: AllegroSettings = AllegroSettings()
    logging: LoggingSettings = LoggingSettings()
    sync: SyncSettings = SyncSettings()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
    
    def log_configuration(self) -> Dict[str, Any]:
        """Возвращает конфигурацию для логирования"""
        return {
            "environment_variables": log_environment_variables(),
            "database": {
                "host": self.database.host,
                "port": self.database.port,
                "name": self.database.name,
                "user": self.database.user,
                "url": mask_sensitive_value("url", self.database.url)
            },
            "redis": {
                "host": self.redis.host,
                "port": self.redis.port,
                "db": self.redis.db,
                "url": mask_sensitive_value("url", self.redis.url)
            },
            "api": {
                "host": self.api.host,
                "port": self.api.port,
                "debug": self.api.debug,
                "prefix": self.api.prefix,
                "secret_key": mask_sensitive_value("secret_key", self.api.secret_key)
            },
            "celery": {
                "broker_url": mask_sensitive_value("broker_url", self.celery.broker_url),
                "result_backend": mask_sensitive_value("result_backend", self.celery.result_backend),
                "timezone": self.celery.timezone
            },
            "allegro": {
                "client_id": mask_sensitive_value("client_id", self.allegro.client_id),
                "client_secret": mask_sensitive_value("client_secret", self.allegro.client_secret),
                "api_url": self.allegro.api_url,
                "sandbox_mode": self.allegro.sandbox_mode
            },
            "logging": {
                "level": self.logging.level,
                "file_path": self.logging.file_path,
                "format": self.logging.format
            }
        }


# Глобальный экземпляр настроек
settings = Settings() 