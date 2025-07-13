"""
@file: app/models/user_token.py
@description: Модель токенов пользователей Allegro
@dependencies: sqlmodel, pydantic
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlmodel import SQLModel, Field

from .base import BaseModel


class UserToken(BaseModel, table=True):
    """Модель токена пользователя в базе данных"""
    
    __tablename__ = "user_tokens"
    
    user_id: str = Field(
        index=True,
        description="Внешний идентификатор пользователя"
    )
    
    allegro_token: str = Field(
        description="Access token для Allegro API"
    )
    
    refresh_token: str = Field(
        description="Refresh token для обновления access token"
    )
    
    expires_at: datetime = Field(
        description="Время истечения access token"
    )
    
    is_active: bool = Field(
        default=True,
        description="Активен ли токен"
    )


class UserTokenCreate(SQLModel):
    """Схема для создания нового токена"""
    user_id: str
    allegro_token: str
    refresh_token: str
    expires_at: datetime
    is_active: bool = True


class UserTokenRead(SQLModel):
    """Схема для чтения токена (без чувствительных данных)"""
    
    id: UUID
    user_id: str
    expires_at: datetime
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None


class UserTokenUpdate(SQLModel):
    """Схема для обновления токена"""
    
    allegro_token: Optional[str] = None
    refresh_token: Optional[str] = None
    expires_at: Optional[datetime] = None
    is_active: Optional[bool] = None
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow) 