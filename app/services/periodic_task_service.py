"""
@file: periodic_task_service.py
@description: Сервис для управления периодическими задачами Celery через celery-sqlalchemy-scheduler
@dependencies: celery_sqlalchemy_scheduler, celery_app
"""
from datetime import timedelta
from sqlalchemy_celery_beat.models import PeriodicTask, IntervalSchedule
from sqlalchemy.orm import Session as AlchemySession
from app.celery_app import celery_app
import json

class PeriodicTaskService:
    def __init__(self, db: AlchemySession):
        self.db = db

    def add_periodic_sync_task(self, task_name: str, user_id: str, token_id: str, interval_minutes: int):
        # Найти или создать расписание
        schedule = self.db.query(IntervalSchedule).filter_by(every=interval_minutes, period="minutes").first()
        if not schedule:
            schedule = IntervalSchedule(every=interval_minutes, period="minutes")
            self.db.add(schedule)
            self.db.commit()
            self.db.refresh(schedule)
        # Создать задачу
        periodic_task = PeriodicTask(
            name=task_name,
            task="run_order_sync_task",
            schedule_model=schedule,
            kwargs=json.dumps({
                "user_id": user_id,
                "token_id": token_id,
                "force_full_sync": False
            }),
            enabled=True
        )
        self.db.add(periodic_task)
        self.db.commit()
        self.db.refresh(periodic_task)
        return periodic_task

    def remove_periodic_sync_task(self, task_name: str):
        task = self.db.query(PeriodicTask).filter_by(name=task_name).first()
        if task:
            self.db.delete(task)
            self.db.commit()
            return True
        return False 