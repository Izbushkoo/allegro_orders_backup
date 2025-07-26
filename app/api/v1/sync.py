"""
@file: app/api/v1/sync.py
@description: API эндпоинты для синхронизации заказов
@dependencies: fastapi, pydantic
"""

from os import sync
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from enum import Enum

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlmodel.ext.asyncio.session import AsyncSession
from celery.result import AsyncResult
from sqlmodel import select

from app.api.dependencies import DatabaseSession, CurrentUserDep
from app.core.auth import CurrentUser
from app.services.order_sync_service import OrderSyncService
from app.core.database import get_sync_db_session_direct
from app.core.logging import get_logger
from app.exceptions import ValidationHTTPException
from app.celery_app import celery_app
from app.models.task_history import TaskHistory
from app.services.task_history_service import TaskHistoryService
from pydantic import BaseModel, Field
from app.services.active_sync_schedule_service import ActiveSyncScheduleService
from app.services.periodic_task_service import PeriodicTaskService
from sqlalchemy.orm import Session as AlchemySession
from app.core.database import get_alchemy_session

logger = get_logger(__name__)
router = APIRouter()

# Enums

class SyncStatus(str, Enum):
    """Статус синхронизации"""
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

# Pydantic модели для API

class SyncTrigger(BaseModel):
    """Модель для запуска синхронизации"""
    token_id: UUID = Field(..., description="ID токена для синхронизации (обязательный параметр)")
    sync_from_date: Optional[datetime] = Field(None, description="Синхронизация с даты")
    force_full_sync: bool = Field(False, description="Принудительная полная синхронизация")

class SyncResponse(BaseModel):
    """Модель ответа синхронизации"""
    id: UUID
    token_id: UUID
    sync_started_at: datetime
    sync_completed_at: Optional[datetime]
    sync_status: SyncStatus
    orders_processed: int
    orders_added: int
    orders_updated: int
    error_message: Optional[str]
    sync_from_date: Optional[datetime]
    sync_to_date: Optional[datetime]
    created_at: datetime
    updated_at: Optional[datetime]

class SyncList(BaseModel):
    """Модель списка синхронизаций"""
    syncs: List[SyncResponse]
    total: int
    page: int
    per_page: int

class SyncStats(BaseModel):
    """Статистика синхронизации"""
    total_syncs: int
    successful_syncs: int
    failed_syncs: int
    running_syncs: int
    total_orders_processed: int
    total_orders_added: int
    total_orders_updated: int
    last_sync_date: Optional[datetime]
    average_sync_duration: Optional[float]

class SyncTaskResponse(BaseModel):
    """Ответ о запуске задачи синхронизации"""
    task_id: str
    status: str
    message: str
    started_at: datetime

class TaskHistoryRead(BaseModel):
    id: UUID
    task_id: str
    user_id: str
    task_type: str
    status: str
    params: Dict[str, Any]
    result: Optional[Dict[str, Any]]
    error: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]
    updated_at: datetime
    description: Optional[str]
    progress: Optional[float]
    parent_task_id: Optional[str]

    class Config:
        orm_mode = True

class ActivateSyncRequest(BaseModel):
    token_id: UUID
    interval_minutes: int = Field(..., ge=1, le=1440, description="Интервал автосинхронизации в минутах (1-1440)")

class DeactivateSyncRequest(BaseModel):
    token_id: UUID

class TokenSyncStatusResponse(BaseModel):
    """Статус автосинхронизации для конкретного токена"""
    token_id: str
    is_active: bool
    interval_minutes: Optional[int]
    status: Optional[str]
    task_name: Optional[str]
    last_run_at: Optional[datetime]
    last_success_at: Optional[datetime]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

# API Endpoints

@router.post("/start", response_model=Dict[str, Any], summary="Запустить синхронизацию")
async def start_sync(
    sync_params: SyncTrigger,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Запустить синхронизацию заказов для текущего пользователя через Celery.
    Если указать параметр sync_from_date будут получены заказы с этой даты, без фактического получения событий заказов.
    Если параметр sync_from_date не указан, будут получены события заказов с момента последнего имеющегося в базе события заказа.
    
    **Требует аутентификации через JWT токен.**
    
    Выполняет асинхронную синхронизацию заказов с Allegro API через Celery.
    """
    try:
        logger.info(f"Запуск синхронизации (через Celery) для пользователя {current_user.user_id} с токеном {sync_params.token_id}")
        
        # Проверяем что токен принадлежит пользователю
        from app.services.allegro_auth_service import AllegroAuthService
        auth_service = AllegroAuthService(None)
        token_record = auth_service.get_token_by_id_sync(str(sync_params.token_id), current_user.user_id)
        
        if not token_record:
            raise ValidationHTTPException(
                detail=f"Токен {sync_params.token_id} не найден или не принадлежит пользователю"
            )
        
        # Формируем параметры для Celery задачи
        celery_kwargs = {
            "user_id": str(current_user.user_id),
            "token_id": str(sync_params.token_id),
            "sync_from_date": sync_params.sync_from_date.isoformat() if sync_params.sync_from_date else None,
            "force_full_sync": sync_params.force_full_sync
        }
        
        # Импортируем задачу Celery
        from app.tasks.sync_tasks import run_order_sync_task
        from datetime import datetime
        task = run_order_sync_task.delay(**celery_kwargs)

        started_at = datetime.utcnow()
        
        logger.info(f"Celery задача синхронизации отправлена: task_id={task.id}")
        
        return {
            "success": True,
            "message": "Синхронизация запущена через Celery",
            "task_id": task.id,
            "user_id": current_user.user_id,
            "token_id": str(sync_params.token_id),
            "sync_type": "full" if sync_params.force_full_sync else "incremental",
            "started_at": started_at.isoformat(),
            "status": "PENDING"
        }
        
    except Exception as e:
        logger.error(f"Ошибка запуска синхронизации через Celery: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка запуска синхронизации: {str(e)}")


@router.get("/history", response_model=SyncList, summary="История синхронизаций")
async def get_sync_history(
    page: int = Query(1, ge=1, description="Номер страницы"),
    per_page: int = Query(10, ge=1, le=100, description="Элементов на странице"),
    token_id: Optional[UUID] = Query(None, description="ID токена"),
    status: Optional[SyncStatus] = Query(None, description="Статус синхронизации"),
    date_from: Optional[datetime] = Query(None, description="Период с"),
    date_to: Optional[datetime] = Query(None, description="Период по"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить историю синхронизаций для текущего пользователя.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        logger.info(f"Получение истории синхронизации для пользователя {current_user.user_id}")
        
        db_session = get_sync_db_session_direct()
        sync_service = OrderSyncService(db_session)
        
        result = sync_service.get_user_sync_history(
            user_id=current_user.user_id,
            page=page,
            per_page=per_page,
            token_id=token_id,
            status=status,
            date_from=date_from,
            date_to=date_to
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка получения истории синхронизации: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения истории: {str(e)}")



@router.get("/stats", response_model=SyncStats, summary="Статистика синхронизаций")
async def get_sync_stats(
    date_from: Optional[datetime] = Query(None, description="Период с"),
    date_to: Optional[datetime] = Query(None, description="Период по"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить статистику синхронизаций для текущего пользователя.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        logger.info(f"Получение статистики синхронизации для пользователя {current_user.user_id}")
        
        db_session = get_sync_db_session_direct()
        sync_service = OrderSyncService(db_session)
        
        stats = sync_service.get_user_sync_stats(
            user_id=current_user.user_id,
            date_from=date_from,
            date_to=date_to
        )
        
        return stats
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики синхронизации: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения статистики: {str(e)}")


@router.get("/running", response_model=List[SyncResponse], summary="Активные синхронизации")
async def get_running_syncs(
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить список активных синхронизаций для текущего пользователя.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        logger.info(f"Получение активных синхронизаций для пользователя {current_user.user_id}")
        
        db_session = get_sync_db_session_direct()
        sync_service = OrderSyncService(db_session)
        
        running_syncs = sync_service.get_running_syncs(user_id=current_user.user_id)
        
        return running_syncs
        
    except Exception as e:
        logger.error(f"Ошибка получения активных синхронизаций: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения активных синхронизаций: {str(e)}")


@router.get("/task/{task_id}", response_model=Dict[str, Any], summary="Статус задачи Celery")
async def get_task_status(
    task_id: str,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить статус задачи синхронизации Celery. Результат из бекенда Celery.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        logger.info(f"Получение статуса задачи {task_id} для пользователя {current_user.user_id}")
        
        task_result = AsyncResult(task_id, app=celery_app)
        
        return {
            "task_id": task_id,
            "status": task_result.status,
            "result": task_result.result if task_result.successful() else None,
            "info": task_result.info,
            "user_id": current_user.user_id
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения статуса задачи: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения статуса задачи: {str(e)}") 

@router.get("/tasks/history", response_model=List[TaskHistoryRead], summary="История всех задач пользователя")
async def get_task_history(
    status: Optional[str] = Query(None, description="Статус задачи (PENDING, SUCCESS, FAILURE и т.д.)"),
    task_type: Optional[str] = Query(None, description="Тип задачи (order_sync, offer_update и т.д.)"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    per_page: int = Query(20, ge=1, le=100, description="Элементов на странице"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить историю всех задач пользователя (любого типа).
    """
    try:
        db_session = get_sync_db_session_direct()
        statement = select(TaskHistory).where(TaskHistory.user_id == current_user.user_id)
        if status:
            statement = statement.where(TaskHistory.status == status)
        if task_type:
            statement = statement.where(TaskHistory.task_type == task_type)
        statement = statement.order_by(TaskHistory.started_at.desc())
        tasks = db_session.exec(statement.offset((page-1)*per_page).limit(per_page)).all()
        db_session.close()
        return tasks
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения истории задач: {str(e)}")

@router.get("/tasks/active", response_model=List[TaskHistoryRead], summary="Активные задачи пользователя")
async def get_active_tasks(
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить все активные (не завершённые) задачи пользователя.
    """
    try:
        db_session = get_sync_db_session_direct()
        statement = select(TaskHistory).where(
            TaskHistory.user_id == current_user.user_id,
            TaskHistory.status.in_(["PENDING", "STARTED"])
        ).order_by(TaskHistory.started_at.desc())
        tasks = db_session.exec(statement).all()
        db_session.close()
        return tasks
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения активных задач: {str(e)}")

@router.get("/tasks/{task_id}", response_model=TaskHistoryRead, summary="Детали задачи по task_id")
async def get_task_details(
    task_id: str,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить подробную информацию о задаче по её task_id.
    """
    try:
        db_session = get_sync_db_session_direct()
        statement = select(TaskHistory).where(
            TaskHistory.task_id == task_id,
            TaskHistory.user_id == current_user.user_id
        )
        task = db_session.exec(statement).first()
        db_session.close()
        if not task:
            raise HTTPException(status_code=404, detail="Задача не найдена")
        return task
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения статуса задачи: {str(e)}") 

@router.post("/tasks/revoke", summary="Отменить задачу по task_id")
async def revoke_task(
    task_id: str,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Отменить Celery задачу по task_id (только если принадлежит пользователю).
    """
    db_session = get_sync_db_session_direct()
    task_service = TaskHistoryService(db_session)
    task = task_service.get_task_by_id(task_id)
    if not task or task.user_id != current_user.user_id:
        db_session.close()
        raise HTTPException(status_code=404, detail="Задача не найдена или не принадлежит пользователю")
    # Отправляем revoke в Celery
    celery_app.control.revoke(task_id, terminate=True)
    # Обновляем статус в БД
    task_service.revoke_task(task_id, current_user.user_id)
    db_session.close()
    return {"message": "Задача отменена", "task_id": task_id, "status": "REVOKED"}

@router.get("/tasks/{task_id}/result", summary="Подробный результат задачи")
async def get_task_result(
    task_id: str,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить подробный результат задачи по task_id (только если принадлежит пользователю).
    """
    db_session = get_sync_db_session_direct()
    task_service = TaskHistoryService(db_session)
    result = task_service.get_task_result(task_id, current_user.user_id)
    db_session.close()
    if not result:
        raise HTTPException(status_code=404, detail="Задача не найдена или не принадлежит пользователю")
    return result 

@router.post("/activate", summary="Включить автосинхронизацию для токена")
async def activate_sync(
    req: ActivateSyncRequest,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Включить автосинхронизацию для токена (создать периодическую задачу в Celery Beat).
    """
    db = get_sync_db_session_direct()
    alchemy_db = get_alchemy_session()
    schedule_service = ActiveSyncScheduleService(db)
    periodic_service = PeriodicTaskService(alchemy_db)
    # Проверка: не активирована ли уже
    if schedule_service.get_by_token(current_user.user_id, str(req.token_id)):
        db.close()
        alchemy_db.close()
        raise HTTPException(status_code=400, detail="Автосинхронизация уже активна для этого токена")
    task_name = f"sync_{req.token_id}_periodic"
    # Создать задачу в Beat
    periodic_service.add_periodic_sync_task(
        task_name=task_name,
        user_id=str(current_user.user_id),
        token_id=str(req.token_id),
        interval_minutes=req.interval_minutes
    )
    # Сохранить в мониторинговой таблице
    schedule_service.create(
        user_id=current_user.user_id,
        token_id=str(req.token_id),
        interval_minutes=req.interval_minutes,
        task_name=task_name
    )
    db.close()
    alchemy_db.close()
    return {"message": "Автосинхронизация активирована", "token_id": str(req.token_id), "interval_minutes": req.interval_minutes}

@router.post("/deactivate", summary="Отключить автосинхронизацию для токена")
async def deactivate_sync(
    req: DeactivateSyncRequest,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Отключить автосинхронизацию для токена (удалить периодическую задачу из Celery Beat).
    """
    db = get_sync_db_session_direct()
    alchemy_db = get_alchemy_session()
    schedule_service = ActiveSyncScheduleService(db)
    periodic_service = PeriodicTaskService(alchemy_db)
    schedule = schedule_service.get_by_token(current_user.user_id, str(req.token_id))
    if not schedule:
        db.close()
        alchemy_db.close()
        raise HTTPException(status_code=404, detail="Автосинхронизация не найдена для этого токена")
    # Удалить задачу из Beat
    periodic_service.remove_periodic_sync_task(schedule.task_name)
    # Деактивировать в мониторинговой таблице
    schedule_service.delete(current_user.user_id, str(req.token_id))
    db.close()
    alchemy_db.close()
    return {"message": "Автосинхронизация отключена", "token_id": str(req.token_id)}

@router.get("/active", summary="Список активных автосинхронизаций пользователя")
async def get_active_syncs(
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить список активных автосинхронизаций пользователя.
    """
    db = get_sync_db_session_direct()
    schedule_service = ActiveSyncScheduleService(db)
    active = schedule_service.get_by_user(current_user.user_id)
    db.close()
    return [
        {
            "token_id": s.token_id,
            "interval_minutes": s.interval_minutes,
            "status": s.status,
            "last_run_at": s.last_run_at,
            "last_success_at": s.last_success_at,
            "created_at": s.created_at,
            "updated_at": s.updated_at
        }
        for s in active
    ]

@router.get("/status/{token_id}", response_model=TokenSyncStatusResponse, summary="Статус автосинхронизации для конкретного токена")
async def get_sync_status_for_token(
    token_id: UUID,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить статус автосинхронизации для конкретного токена.
    Возвращает информацию о том, активна ли автосинхронизация для данного токена.
    """
    try:
        logger.info(f"Получение статуса автосинхронизации для токена {token_id} пользователя {current_user.user_id}")
        
        db = get_sync_db_session_direct()
        schedule_service = ActiveSyncScheduleService(db)
        
        # Проверяем, принадлежит ли токен пользователю
        from app.services.allegro_auth_service import AllegroAuthService
        auth_service = AllegroAuthService(None)
        token_record = auth_service.get_token_by_id_sync(str(token_id), current_user.user_id)
        
        if not token_record:
            db.close()
            raise HTTPException(status_code=404, detail="Токен не найден или не принадлежит пользователю")
        
        # Получаем статус автосинхронизации
        schedule = schedule_service.get_by_token(current_user.user_id, str(token_id))
        
        if schedule:
            # Автосинхронизация активна
            result = {
                "token_id": str(token_id),
                "is_active": True,
                "interval_minutes": schedule.interval_minutes,
                "status": schedule.status,
                "task_name": schedule.task_name,
                "last_run_at": schedule.last_run_at,
                "last_success_at": schedule.last_success_at,
                "created_at": schedule.created_at,
                "updated_at": schedule.updated_at
            }
        else:
            # Автосинхронизация не активна
            result = {
                "token_id": str(token_id),
                "is_active": False,
                "interval_minutes": None,
                "status": None,
                "task_name": None,
                "last_run_at": None,
                "last_success_at": None,
                "created_at": None,
                "updated_at": None
            }
        
        db.close()
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка получения статуса автосинхронизации для токена: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения статуса: {str(e)}") 