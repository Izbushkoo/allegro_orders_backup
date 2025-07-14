"""
@file: active_sync_schedule_service.py
@description: Сервис для управления автосинхронизацией (ActiveSyncSchedule)
@dependencies: ActiveSyncSchedule, sqlmodel
"""
from datetime import datetime
from typing import Optional, List
from uuid import UUID
from sqlmodel import Session, select
from app.models.active_sync_schedule import ActiveSyncSchedule

class ActiveSyncScheduleService:
    def __init__(self, db: Session):
        self.db = db

    def create(self, user_id: UUID, token_id: str, interval_minutes: int, task_name: str) -> ActiveSyncSchedule:
        now = datetime.utcnow()
        schedule = ActiveSyncSchedule(
            user_id=user_id,
            token_id=token_id,
            interval_minutes=interval_minutes,
            status="active",
            task_name=task_name,
            created_at=now,
            updated_at=now
        )
        self.db.add(schedule)
        self.db.commit()
        self.db.refresh(schedule)
        return schedule

    def delete(self, user_id: UUID, token_id: str) -> bool:
        schedule = self.db.exec(
            select(ActiveSyncSchedule).where(
                ActiveSyncSchedule.user_id == user_id,
                ActiveSyncSchedule.token_id == token_id,
                ActiveSyncSchedule.status == "active"
            )
        ).first()
        if not schedule:
            return False
        schedule.status = "inactive"
        schedule.updated_at = datetime.utcnow()
        self.db.add(schedule)
        self.db.commit()
        return True

    def get_by_user(self, user_id: UUID) -> List[ActiveSyncSchedule]:
        return self.db.exec(
            select(ActiveSyncSchedule).where(
                ActiveSyncSchedule.user_id == user_id,
                ActiveSyncSchedule.status == "active"
            ).order_by(ActiveSyncSchedule.created_at.desc())
        ).all()

    def get_by_token(self, user_id: UUID, token_id: str) -> Optional[ActiveSyncSchedule]:
        return self.db.exec(
            select(ActiveSyncSchedule).where(
                ActiveSyncSchedule.user_id == user_id,
                ActiveSyncSchedule.token_id == token_id,
                ActiveSyncSchedule.status == "active"
            )
        ).first()

    def update_last_run(self, user_id: UUID, token_id: str):
        schedule = self.get_by_token(user_id, token_id)
        if schedule:
            schedule.last_run_at = datetime.utcnow()
            schedule.updated_at = datetime.utcnow()
            self.db.add(schedule)
            self.db.commit()

    def update_last_success(self, user_id: UUID, token_id: str):
        schedule = self.get_by_token(user_id, token_id)
        if schedule:
            schedule.last_success_at = datetime.utcnow()
            schedule.updated_at = datetime.utcnow()
            self.db.add(schedule)
            self.db.commit() 