"""
@file: app/models/order_technical_flags.py
@description: Модель для отслеживания технических состояний заказов (флаги стока, инвойсов)
@dependencies: sqlmodel, pydantic, uuid
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlmodel import SQLModel, Field, UniqueConstraint
from pydantic import BaseModel

from .base import BaseModel as BaseDBModel


class OrderTechnicalFlagsBase(SQLModel):
    """Базовые поля технических флагов заказа"""
    
    token_id: UUID = Field(
        foreign_key="user_tokens.id",
        description="ID токена пользователя"
    )
    
    allegro_order_id: str = Field(
        index=True,
        description="ID заказа в системе Allegro"
    )
    
    is_stock_updated: bool = Field(
        default=False,
        description="Было ли произведено списание стока по данному заказу"
    )
    
    has_invoice_created: bool = Field(
        default=False,
        description="Был ли создан инвойс для данного заказа"
    )
    
    invoice_id: Optional[str] = Field(
        default=None,
        description="ID созданного инвойса (если есть)"
    )


class OrderTechnicalFlags(OrderTechnicalFlagsBase, BaseDBModel, table=True):
    """Модель технических флагов заказа в базе данных"""
    
    __tablename__ = "order_technical_flags"
    
    # Уникальный констрейнт для предотвращения дублирования флагов per-order
    __table_args__ = (
        UniqueConstraint("token_id", "allegro_order_id", name="uq_order_technical_flags_per_order"),
    )
    
    class Config:
        """Конфигурация модели"""
        arbitrary_types_allowed = True


# API модели для работы с техническими флагами

class OrderTechnicalFlagsCreate(BaseModel):
    """Модель для создания технических флагов заказа"""
    allegro_order_id: str
    is_stock_updated: bool = False
    has_invoice_created: bool = False
    invoice_id: Optional[str] = None


class OrderTechnicalFlagsRead(BaseModel):
    """Модель для чтения технических флагов заказа"""
    id: int
    token_id: UUID
    allegro_order_id: str
    is_stock_updated: bool
    has_invoice_created: bool
    invoice_id: Optional[str]
    created_at: datetime
    updated_at: datetime


class OrderTechnicalFlagsUpdate(BaseModel):
    """Модель для обновления технических флагов заказа"""
    is_stock_updated: Optional[bool] = None
    has_invoice_created: Optional[bool] = None  
    invoice_id: Optional[str] = None


class StockStatusUpdate(BaseModel):
    """Модель для обновления статуса списания стока"""
    is_stock_updated: bool


class InvoiceStatusUpdate(BaseModel):
    """Модель для обновления статуса инвойса"""
    has_invoice_created: bool
    invoice_id: Optional[str] = None


class OrderWithTechnicalFlags(BaseModel):
    """Модель заказа с техническими флагами"""
    order_data: dict
    technical_flags: OrderTechnicalFlagsRead 