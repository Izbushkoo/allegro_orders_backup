"""
@file: sync_tasks.py
@description: Celery задачи для синхронизации заказов Allegro
@dependencies: celery_app, OrderSyncService, TaskHistoryService
"""
from celery import shared_task
from datetime import datetime
from typing import Optional
from app.services.order_sync_service import OrderSyncService
from app.core.database import get_sync_db_session_direct
from app.services.task_history_service import TaskHistoryService
import logging

logger = logging.getLogger(__name__)

@shared_task(bind=True, name="run_order_sync_task")
def run_order_sync_task(self, user_id: str, token_id: str, sync_from_date: Optional[str] = None, force_full_sync: bool = False):
    """
    Celery задача для асинхронной синхронизации заказов Allegro.
    Args:
        user_id: ID пользователя
        token_id: ID токена
        sync_from_date: дата начала синхронизации (ISO str)
        force_full_sync: принудительная полная синхронизация
    Returns:
        dict: результат синхронизации
    """
    db_session = get_sync_db_session_direct()
    task_history = TaskHistoryService(db_session)
    params = {
        "user_id": user_id,
        "token_id": token_id,
        "sync_from_date": sync_from_date,
        "force_full_sync": force_full_sync
    }
    # 1. Создаём запись о задаче (если не существует)
    task_history.create_task(
        task_id=self.request.id,
        user_id=user_id,
        task_type="order_sync",
        params=params,
        description="Синхронизация заказов Allegro"
    )
    try:
        logger.info(f"[Celery] Запуск синхронизации для user_id={user_id}, token_id={token_id}, sync_from_date={sync_from_date}, force_full_sync={force_full_sync}")
        sync_service = OrderSyncService(db_session, user_id, token_id)
        dt_from = None
        if sync_from_date:
            dt_from = datetime.fromisoformat(sync_from_date)
        result = sync_service.sync_orders_safe(
            full_sync=force_full_sync,
            sync_from_date=dt_from
        )
        logger.info(f"[Celery] Синхронизация завершена для user_id={user_id}, token_id={token_id}")
        # 2. Обновляем запись о задаче (успех)
        task_history.update_task(
            task_id=self.request.id,
            status="SUCCESS" if result["success"] else "FAILURE",
            result=result,
            error=None if result["success"] else str(result.get("critical_issues")),
            finished=True
        )
        db_session.close()
        return {"success": result["success"], "statistics": result, "error": None}
    except Exception as e:
        logger.error(f"[Celery] Ошибка синхронизации: {e}")
        # 3. Обновляем запись о задаче (ошибка)
        task_history.update_task(
            task_id=self.request.id,
            status="FAILURE",
            result=None,
            error=str(e),
            finished=True
        )
        db_session.close()
        return {"success": False, "statistics": None, "error": str(e)} 