"""
@file: app/models/order.py
@description: Модель заказов из Allegro API
@dependencies: sqlmodel, pydantic, json
"""

from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID

from sqlmodel import SQLModel, Field, Relationship, Column, JSON, UniqueConstraint

from .base import BaseModel


class OrderBase(SQLModel):
    """Базовые поля заказа"""
    
    token_id: UUID = Field(
        foreign_key="user_tokens.id",
        description="ID токена пользователя"
    )
    
    allegro_order_id: str = Field(
        index=True,
        description="ID заказа в системе Allegro (уникальный для токена)"
    )
    
    order_data: Dict[str, Any] = Field(
        sa_column=Column(JSON),
        description="Полные данные заказа в формате JSON"
    )
    
    order_date: datetime = Field(
        index=True,
        description="Дата создания заказа в Allegro"
    )
    
    is_deleted: bool = Field(
        default=False,
        description="Помечен ли заказ как удаленный"
    )



class Order(OrderBase, BaseModel, table=True):
    """Модель заказа в базе данных"""
    
    __tablename__ = "orders"
    
    # Уникальный констрейнт для предотвращения дублирования заказов per-token
    __table_args__ = (
        UniqueConstraint("token_id", "allegro_order_id", name="uq_orders_per_token"),
    )
    
    # Связи с другими таблицами (закомментировано для отладки)
    # user_token: "UserToken" = Relationship(back_populates="orders")
    
    class Config:
        """Конфигурация модели"""
        arbitrary_types_allowed = True


class OrderCreate(SQLModel):
    """Схема для создания нового заказа"""
    token_id: UUID
    allegro_order_id: str
    order_data: Dict[str, Any]
    order_date: datetime
    is_deleted: bool = False


class OrderRead(SQLModel):
    """Схема для чтения заказа"""
    
    id: UUID
    token_id: UUID
    allegro_order_id: str
    order_data: Dict[str, Any]
    order_date: datetime
    is_deleted: bool
    created_at: datetime
    updated_at: Optional[datetime] = None


class OrderUpdate(SQLModel):
    """Схема для обновления заказа"""
    
    order_data: Optional[Dict[str, Any]] = None
    order_date: Optional[datetime] = None
    is_deleted: Optional[bool] = None
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class OrderSummary(SQLModel):
    """Сводная информация о заказе для списков"""
    
    id: UUID
    allegro_order_id: str
    order_date: datetime
    status: Optional[str] = None
    buyer_login: Optional[str] = None
    total_amount: Optional[float] = None
    currency: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None 