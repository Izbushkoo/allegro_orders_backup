"""
@file: app/models/order_event.py
@description: Модель событий заказов из Allegro API
@dependencies: sqlmodel, pydantic, json
"""

from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID

from sqlmodel import SQLModel, Field, Relationship, Column, JSON, UniqueConstraint

from .base import BaseModel


class OrderEventBase(SQLModel):
    """Базовые поля события заказа"""
    
    order_id: str = Field(
        index=True,
        description="ID заказа в системе Allegro"
    )
    
    token_id: UUID = Field(
        foreign_key="user_tokens.id",
        description="ID токена пользователя"
    )
    
    event_type: str = Field(
        index=True,
        description="Тип события (ORDER_STATUS_CHANGED, PAYMENT_STATUS_CHANGED, etc.)"
    )
    
    occurred_at: datetime = Field(
        index=True,
        description="Время когда произошло событие в Allegro"
    )
    
    event_data: Dict[str, Any] = Field(
        sa_column=Column(JSON),
        description="Полные данные события в формате JSON"
    )
    
    event_id: Optional[str] = Field(
        default=None,
        index=True,
        description="ID события для pagination в Allegro API (параметр from)"
    )
    
    is_duplicate: bool = Field(
        default=False,
        description="Помечено ли событие как дубликат"
    )


class OrderEvent(OrderEventBase, BaseModel, table=True):
    """Модель события заказа в базе данных"""
    
    __tablename__ = "order_events"
    
    # Уникальные констрейнты для предотвращения дублирования событий per-token
    __table_args__ = (
        UniqueConstraint("token_id", "event_id", name="uq_order_events_per_token"),
        UniqueConstraint("token_id", "order_id", "event_type", "occurred_at", name="uq_order_events_composite"),
    )
    
    # Связи с другими таблицами (закомментировано для отладки)
    # user_token: "UserToken" = Relationship(back_populates="order_events")
    
    class Config:
        """Конфигурация модели"""
        arbitrary_types_allowed = True


class OrderEventCreate(SQLModel):
    """Схема для создания нового события заказа"""
    order_id: str
    token_id: UUID
    event_type: str
    occurred_at: datetime
    event_data: Dict[str, Any]
    event_id: Optional[str] = None  # Опциональное поле для pagination


class OrderEventRead(SQLModel):
    """Схема для чтения события заказа"""
    
    id: UUID
    order_id: str
    token_id: UUID
    event_type: str
    occurred_at: datetime
    event_data: Dict[str, Any]
    event_id: Optional[str] = None  # Опциональное поле для pagination
    is_duplicate: bool = False
    created_at: datetime
    processed_at: datetime = Field(alias="created_at")


class OrderEventUpdate(SQLModel):
    """Схема для обновления события заказа"""
    
    event_data: Optional[Dict[str, Any]] = None
    is_duplicate: Optional[bool] = None
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
   