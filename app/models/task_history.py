"""
@file: task_history.py
@description: Универсальная модель истории Celery задач (любого типа)
@dependencies: sqlmodel, pydantic
"""

from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID, uuid4
from sqlmodel import SQLModel, Field, Column, JSON

class TaskHistory(SQLModel, table=True):
    __tablename__ = "task_history"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)
    task_id: str = Field(index=True, unique=True, description="Celery task id")
    user_id: str = Field(index=True, description="ID пользователя (строка)")
    task_type: str = Field(index=True, description="Тип задачи (order_sync, offer_update и т.д.)")
    status: str = Field(index=True, description="Статус задачи (PENDING, STARTED, SUCCESS, FAILURE, REVOKED)")
    params: Dict[str, Any] = Field(sa_column=Column(JSON), description="Параметры задачи (JSON)")
    result: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON), description="Результат выполнения (JSON)")
    error: Optional[str] = Field(default=None, description="Ошибка, если была")
    started_at: datetime = Field(default_factory=datetime.utcnow, description="Время запуска")
    finished_at: Optional[datetime] = Field(default=None, description="Время завершения")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Время последнего обновления")
    description: Optional[str] = Field(default=None, description="Описание задачи для пользователя")
    progress: Optional[float] = Field(default=None, description="Прогресс выполнения (0..1)")
    parent_task_id: Optional[str] = Field(default=None, description="ID родительской задачи, если есть") 