"""
@file: app/models/base.py
@description: Базовая модель с общими полями для всех таблиц
@dependencies: sqlmodel, uuid, datetime
"""

from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import SQLModel, Field


class BaseModel(SQLModel):
    """Базовая модель с общими полями"""
    
    id: Optional[UUID] = Field(
        default_factory=uuid4,
        primary_key=True,
        description="Уникальный идентификатор записи"
    )
    
    created_at: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        description="Время создания записи"
    )
    
    updated_at: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        description="Время последнего обновления записи"
    )
    
    model_config = {
        "from_attributes": True,
        "use_enum_values": True,
        "validate_assignment": False,
        "extra": "ignore"
    } 