"""
@file: active_sync_schedule.py
@description: Модель для хранения активных автосинхронизаций (periodic tasks для Celery Beat)
@dependencies: sqlmodel, pydantic
"""
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4
from sqlmodel import SQLModel, Field

class ActiveSyncSchedule(SQLModel, table=True):
    __tablename__ = "active_sync_schedules"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    user_id: str = Field(index=True, description="ID пользователя (строка)")
    token_id: str = Field(index=True)
    interval_minutes: int
    status: str = Field(default="active")  # active/inactive
    task_name: str = Field(index=True)
    last_run_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow) 