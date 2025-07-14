"""
@file: task_history_service.py
@description: Сервис для работы с историей Celery задач (TaskHistory)
@dependencies: TaskHistory, sqlmodel
"""
import json
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID
from sqlmodel import Session, select
from app.models.task_history import TaskHistory

class TaskHistoryService:
    def __init__(self, db: Session):
        self.db = db

    def create_task(self, *, task_id: str, user_id: UUID, task_type: str, params: Dict[str, Any], description: Optional[str] = None, parent_task_id: Optional[str] = None) -> TaskHistory:
        now = datetime.utcnow()
        task = TaskHistory(
            task_id=task_id,
            user_id=user_id,
            task_type=task_type,
            status="PENDING",
            params=params,
            result=None,
            error=None,
            started_at=now,
            updated_at=now,
            description=description,
            progress=None,
            parent_task_id=parent_task_id
        )
        self.db.add(task)
        self.db.commit()
        self.db.refresh(task)
        return task

    def update_task(self, task_id: str, **kwargs):
        task = self.db.exec(select(TaskHistory).where(TaskHistory.task_id == task_id)).first()
        if not task:
            return None
        for key, value in kwargs.items():
            if not hasattr(task, key):
                continue  # пропускаем несуществующие поля
            if key == "result" and value is not None:
                # Сериализуем datetime в строку
                def default_serializer(obj):
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    raise TypeError(f"Type {type(obj)} not serializable")
                value = json.loads(json.dumps(value, default=default_serializer))
            setattr(task, key, value)
        self.db.commit()
        self.db.refresh(task)
        return task

    def get_task_by_id(self, task_id: str) -> Optional[TaskHistory]:
        """Получить задачу по task_id"""
        return self.db.exec(select(TaskHistory).where(TaskHistory.task_id == task_id)).first()

    def get_tasks_by_user(self, user_id: UUID, limit: int = 50) -> list[TaskHistory]:
        """Получить список задач пользователя (по user_id)"""
        return self.db.exec(
            select(TaskHistory).where(TaskHistory.user_id == user_id).order_by(TaskHistory.started_at.desc()).limit(limit)
        ).all()

    def get_tasks_by_token(self, token_id: str, limit: int = 50) -> list[TaskHistory]:
        """Получить список задач по token_id (ищет в params['token_id'])"""
        return self.db.exec(
            select(TaskHistory)
            .where(TaskHistory.params["token_id"].as_string() == token_id)
            .order_by(TaskHistory.started_at.desc())
            .limit(limit)
        ).all()

    def revoke_task(self, task_id: str, user_id: UUID) -> Optional[TaskHistory]:
        """Отменить задачу (установить статус REVOKED), только если принадлежит user_id"""
        task = self.get_task_by_id(task_id)
        if not task or task.user_id != user_id:
            return None
        task.status = "REVOKED"
        task.finished_at = datetime.utcnow()
        task.updated_at = datetime.utcnow()
        self.db.add(task)
        self.db.commit()
        self.db.refresh(task)
        return task

    def get_task_result(self, task_id: str, user_id: UUID) -> Optional[dict]:
        """Получить подробный результат задачи, если принадлежит user_id"""
        task = self.get_task_by_id(task_id)
        if not task or task.user_id != user_id:
            return None
        return {
            "task_id": task.task_id,
            "status": task.status,
            "result": task.result,
            "error": task.error,
            "progress": task.progress,
            "started_at": task.started_at,
            "finished_at": task.finished_at,
            "updated_at": task.updated_at,
            "params": task.params,
            "description": task.description
        } 