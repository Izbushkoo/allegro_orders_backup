"""
@file: app/models/sync_history.py
@description: Модель истории синхронизации заказов
@dependencies: sqlmodel, pydantic, enum
"""

from datetime import datetime
from typing import Optional
from uuid import UUID
from enum import Enum

from sqlmodel import SQLModel, Field, Relationship

from .base import BaseModel


class SyncStatus(str, Enum):
    """Статусы синхронизации"""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SyncHistoryBase(SQLModel):
    """Базовые поля истории синхронизации"""
    
    token_id: UUID = Field(
        foreign_key="user_tokens.id",
        description="ID токена пользователя"
    )
    
    sync_started_at: datetime = Field(
        description="Время начала синхронизации"
    )
    
    sync_completed_at: Optional[datetime] = Field(
        default=None,
        description="Время завершения синхронизации"
    )
    
    sync_status: SyncStatus = Field(
        default=SyncStatus.RUNNING,
        description="Статус синхронизации"
    )
    
    orders_processed: int = Field(
        default=0,
        description="Количество обработанных заказов"
    )
    
    orders_added: int = Field(
        default=0,
        description="Количество добавленных заказов"
    )
    
    orders_updated: int = Field(
        default=0,
        description="Количество обновленных заказов"
    )
    
    error_message: Optional[str] = Field(
        default=None,
        description="Сообщение об ошибке при неудачной синхронизации"
    )
    
    sync_from_date: Optional[datetime] = Field(
        default=None,
        description="Начальная дата для синхронизации"
    )
    
    sync_to_date: Optional[datetime] = Field(
        default=None,
        description="Конечная дата для синхронизации"
    )


class SyncHistory(SyncHistoryBase, BaseModel, table=True):
    """Модель истории синхронизации в базе данных"""
    
    __tablename__ = "sync_history"
    
    # Связи с другими таблицами (закомментировано для отладки)
    # user_token: "UserToken" = Relationship(back_populates="sync_histories")


class SyncHistoryCreate(SQLModel):
    """Схема для создания новой записи истории синхронизации"""
    token_id: UUID
    sync_started_at: datetime
    sync_completed_at: Optional[datetime] = None
    sync_status: SyncStatus = SyncStatus.RUNNING
    orders_processed: int = 0
    orders_added: int = 0
    orders_updated: int = 0
    error_message: Optional[str] = None
    sync_from_date: Optional[datetime] = None
    sync_to_date: Optional[datetime] = None


class SyncHistoryRead(SQLModel):
    """Схема для чтения истории синхронизации"""
    
    id: UUID
    token_id: UUID
    sync_started_at: datetime
    sync_completed_at: Optional[datetime] = None
    sync_status: SyncStatus
    orders_processed: int
    orders_added: int
    orders_updated: int
    error_message: Optional[str] = None
    sync_from_date: Optional[datetime] = None
    sync_to_date: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None


class SyncHistoryUpdate(SQLModel):
    """Схема для обновления истории синхронизации"""
    
    sync_completed_at: Optional[datetime] = None
    sync_status: Optional[SyncStatus] = None
    orders_processed: Optional[int] = None
    orders_added: Optional[int] = None
    orders_updated: Optional[int] = None
    error_message: Optional[str] = None
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class SyncStatistics(SQLModel):
    """Статистика синхронизации"""
    
    total_syncs: int
    successful_syncs: int
    failed_syncs: int
    total_orders_processed: int
    total_orders_added: int
    total_orders_updated: int
    last_sync_at: Optional[datetime] = None
    avg_sync_duration_seconds: Optional[float] = None 