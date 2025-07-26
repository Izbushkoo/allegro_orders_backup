"""
@file: app/api/v1/orders.py
@description: API эндпоинты для работы с заказами Allegro
@dependencies: fastapi, pydantic
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Query, Path, HTTPException
from pydantic import BaseModel
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.dependencies import CurrentUserDep
from app.core.auth import CurrentUser
from app.services.order_service import OrderService
from app.services.allegro_auth_service import AllegroAuthService
from app.core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()

# Модели данных

# Модели данных удалены - теперь возвращаем полные данные как Dict[str, Any]

class OrderStatistics(BaseModel):
    """Статистика заказов"""
    period_days: int
    total_orders: int
    recent_orders: int
    status_distribution: Dict[str, int]
    financial: Dict[str, Any]
    top_buyers: List[Dict[str, Any]]
    generated_at: str

class SyncResult(BaseModel):
    """Результат синхронизации заказа"""
    success: bool
    order_id: str
    action: str  # created, updated, skipped
    message: str

class SyncHistory(BaseModel):
    """История синхронизации"""
    history: List[Dict[str, Any]]
    total_records: int

class DataQualityReport(BaseModel):
    """Отчет о качестве данных"""
    health_metrics: Dict[str, Any]
    quality_report: Dict[str, Any]
    generated_at: str

class OrderEventsResponse(BaseModel):
    """События заказов от Allegro API"""
    events: List[Dict[str, Any]]
    total_count: int
    has_more: bool
    error: Optional[str] = None

# Вспомогательные функции

def validate_token_and_get_service(token_id: UUID, current_user: CurrentUser) -> OrderService:
    """
    Валидирует принадлежность токена пользователю и создает OrderService.
    
    Args:
        token_id: ID токена
        current_user: Текущий пользователь
        
    Returns:
        OrderService: Инициализированный сервис
        
    Raises:
        HTTPException: Если токен не найден или не принадлежит пользователю
    """
    # Проверяем что токен принадлежит пользователю
    auth_service = AllegroAuthService(None)
    token_record = auth_service.get_token_by_id_sync(str(token_id), current_user.user_id)
    
    if not token_record:
        raise HTTPException(
            status_code=404,
            detail=f"Токен {token_id} не найден или не принадлежит пользователю"
        )
    
    # Создаем OrderService с валидированным токеном
    return OrderService(current_user.user_id, token_id)

# API Endpoints

@router.get("/", 
          response_model=Dict[str, Any], 
          summary="Получить список заказов",
          description="Получение списка заказов с фильтрацией и пагинацией")
async def get_orders(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    limit: int = Query(50, ge=1, le=100, description="Количество заказов на странице"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    status: Optional[str] = Query(None, description="Фильтр по статусу заказа"),
    from_date: Optional[datetime] = Query(None, description="Заказы от указанной даты"),
    to_date: Optional[datetime] = Query(None, description="Заказы до указанной даты"),
    stock_updated: Optional[bool] = Query(None, description="Фильтр по флагу обновления стока"),
    invoice_created: Optional[bool] = Query(None, description="Фильтр по флагу создания инвойса"),
    invoice_id: Optional[str] = Query(None, description="Фильтр по конкретному ID инвойса"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить список заказов текущего пользователя с фильтрацией и пагинацией.
    
    **Фильтрация поддерживается по:**
    - Статусу заказа
    - Диапазону дат
    - Техническим флагам (статус обновления стока, создания инвойсов)
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.get_orders_list(
            limit=limit,
            offset=offset,
            status_filter=status,
            from_date=from_date,
            to_date=to_date,
            stock_updated_filter=stock_updated,
            invoice_created_filter=invoice_created,
            invoice_id_filter=invoice_id
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения заказов: {str(e)}")


@router.get("/search",
          response_model=Dict[str, Any],
          summary="Поиск заказов",
          description="Поиск заказов по email покупателя, ID заказа или имени с дополнительной фильтрацией")
async def search_orders(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    query: str = Query(..., min_length=3, description="Поисковый запрос (минимум 3 символа)"),
    limit: int = Query(50, ge=1, le=1000, description="Максимальное количество результатов"),
    stock_updated: Optional[bool] = Query(None, description="Фильтр по флагу обновления стока"),
    invoice_created: Optional[bool] = Query(None, description="Фильтр по флагу создания инвойса"),
    invoice_id: Optional[str] = Query(None, description="Фильтр по конкретному ID инвойса"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Поиск заказов текущего пользователя по различным критериям.
    
    **Поддерживается поиск по:**
    - Email покупателя
    - ID заказа в Allegro
    - Имени и фамилии покупателя
    - Логину покупателя
    - Названию компании
    
    **Дополнительная фильтрация по техническим флагам:**
    - Статус обновления стока
    - Статус создания инвойсов
    - Конкретный ID инвойса
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.search_orders(
            search_query=query,
            limit=limit,
            stock_updated_filter=stock_updated,
            invoice_created_filter=invoice_created,
            invoice_id_filter=invoice_id
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка поиска заказов: {str(e)}")


@router.get("/statistics",
          response_model=OrderStatistics,
          summary="Статистика заказов",
          description="Получение статистики заказов за указанный период")
async def get_orders_statistics(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    days: int = Query(30, ge=1, le=365, description="Количество дней для анализа"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить статистику заказов текущего пользователя за указанный период.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.get_orders_statistics(
            days=days
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения статистики: {str(e)}")


@router.get("/events",
          response_model=OrderEventsResponse,
          summary="Получить события заказов от Allegro",
          description="Получение событий заказов напрямую от Allegro API")
async def get_order_events(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    limit: int = Query(100, ge=1, le=1000, description="Максимальное количество событий"),
    from_timestamp: Optional[datetime] = Query(None, description="Получить события после указанного времени"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить события заказов от Allegro API для текущего пользователя.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.get_order_events(
            limit=limit,
            from_timestamp=from_timestamp
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения событий: {str(e)}")


@router.get("/sync/history",
          response_model=SyncHistory,
          summary="История синхронизации",
          description="Получение истории синхронизации заказов")
async def get_sync_history(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    limit: int = Query(50, ge=1, le=200, description="Количество записей"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить историю синхронизации заказов для текущего пользователя.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.get_sync_history(
            limit=limit
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения истории синхронизации: {str(e)}")


@router.get("/data-quality",
          response_model=DataQualityReport,
          summary="Отчет о качестве данных",
          description="Получение отчета о качестве данных заказов")
async def get_data_quality_report(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить отчет о качестве данных заказов для текущего пользователя.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.get_data_quality_report()
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения отчета о качестве: {str(e)}")


@router.get("/{order_id}",
          response_model=Dict[str, Any],
          summary="Получить заказ по ID",
          description="Получение детальной информации о заказе")
async def get_order_by_id(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    order_id: str = Path(..., description="ID заказа в Allegro"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить детальную информацию о заказе по его ID.
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может получить только свои заказы.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        order = order_service.get_order_details(order_id)
        
        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")
            
        return order
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения заказа: {str(e)}")


@router.post("/{order_id}/sync",
           response_model=SyncResult,
           summary="Синхронизировать заказ",
           description="Принудительная синхронизация одного заказа")
async def sync_single_order(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    order_id: str = Path(..., description="ID заказа в Allegro"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Принудительно синхронизировать один заказ от Allegro API.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.sync_single_order(
            order_id=order_id
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка синхронизации заказа: {str(e)}")


@router.get("/debug/statuses",
          summary="Доступные статусы заказов",
          description="Получение списка всех статусов заказов в системе")
async def get_available_statuses(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить список всех возможных статусов заказов в системе.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        # Простая заглушка для debug endpoint
        statuses = ["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
        
        return {
            "available_statuses": statuses,
            "description": "Список всех возможных статусов заказов",
            "user_id": current_user.user_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения статусов: {str(e)}")


@router.get("/debug/health",
          summary="Состояние системы заказов",
          description="Проверка состояния системы заказов")
async def get_orders_health(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Проверить состояние системы заказов для текущего пользователя.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        # Простая заглушка для debug endpoint
        from datetime import datetime
        health_status = {
            "status": "healthy",
            "user_id": current_user.user_id,
            "database_connection": "ok",
            "allegro_api_status": "unknown",
            "last_sync": None,
            "orders_count": 0,
            "check_time": datetime.utcnow().isoformat()
        }
        
        return health_status
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка проверки состояния: {str(e)}") 


@router.patch("/{order_id}/stock-status",
            response_model=Dict[str, Any],
            summary="Обновить статус списания стока",
            description="Обновление флага списания стока для заказа")
async def update_stock_status(
    stock_update: Dict[str, bool],  # {"is_stock_updated": true/false}
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    order_id: str = Path(..., description="ID заказа в Allegro"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Обновить статус списания стока для заказа.
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может обновлять только свои заказы.**
    """
    try:
        # Импортируем сервис здесь для избежания циклических импортов
        from app.services.order_technical_flags_service import OrderTechnicalFlagsService
        
        # Валидируем принадлежность токена пользователю
        validate_token_and_get_service(token_id, current_user)
        
        # Валидируем входные данные
        if "is_stock_updated" not in stock_update:
            raise HTTPException(
                status_code=422, 
                detail="Поле 'is_stock_updated' обязательно"
            )
        
        is_stock_updated = stock_update["is_stock_updated"]
        if not isinstance(is_stock_updated, bool):
            raise HTTPException(
                status_code=422,
                detail="Поле 'is_stock_updated' должно быть булевым значением"
            )
        
        # Обновляем флаг стока
        with OrderTechnicalFlagsService(current_user.user_id, token_id) as flags_service:
            updated_flags = flags_service.update_stock_status(order_id, is_stock_updated)
            
            return {
                "success": True,
                "order_id": order_id,
                "is_stock_updated": updated_flags.is_stock_updated,
                "updated_at": updated_flags.updated_at.isoformat(),
                "message": f"Статус списания стока обновлен на {is_stock_updated}"
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка обновления статуса стока: {str(e)}"
        )


@router.patch("/{order_id}/invoice-status",
            response_model=Dict[str, Any],
            summary="Обновить статус создания инвойса",
            description="Обновление флагов создания инвойса для заказа")
async def update_invoice_status(
    invoice_update: Dict[str, Any],  # {"has_invoice_created": true, "invoice_id": "INV-123"}
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    order_id: str = Path(..., description="ID заказа в Allegro"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Обновить статус создания инвойса для заказа.
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может обновлять только свои заказы.**
    """
    try:
        # Импортируем сервис здесь для избежания циклических импортов
        from app.services.order_technical_flags_service import OrderTechnicalFlagsService
        
        # Валидируем принадлежность токена пользователю
        validate_token_and_get_service(token_id, current_user)
        
        # Валидируем входные данные
        if "has_invoice_created" not in invoice_update:
            raise HTTPException(
                status_code=422, 
                detail="Поле 'has_invoice_created' обязательно"
            )
        
        has_invoice_created = invoice_update["has_invoice_created"]
        if not isinstance(has_invoice_created, bool):
            raise HTTPException(
                status_code=422,
                detail="Поле 'has_invoice_created' должно быть булевым значением"
            )
        
        invoice_id = invoice_update.get("invoice_id")
        if invoice_id is not None and not isinstance(invoice_id, str):
            raise HTTPException(
                status_code=422,
                detail="Поле 'invoice_id' должно быть строкой или null"
            )
        
        # Обновляем флаги инвойса
        with OrderTechnicalFlagsService(current_user.user_id, token_id) as flags_service:
            updated_flags = flags_service.update_invoice_status(
                order_id, 
                has_invoice_created, 
                invoice_id
            )
            
            return {
                "success": True,
                "order_id": order_id,
                "has_invoice_created": updated_flags.has_invoice_created,
                "invoice_id": updated_flags.invoice_id,
                "updated_at": updated_flags.updated_at.isoformat(),
                "message": f"Статус инвойса обновлен: created={has_invoice_created}, id={invoice_id}"
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка обновления статуса инвойса: {str(e)}"
        )


@router.get("/{order_id}/technical-flags",
          response_model=Dict[str, Any],
          summary="Получить технические флаги заказа",
          description="Получение текущих технических флагов заказа")
async def get_order_technical_flags(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    order_id: str = Path(..., description="ID заказа в Allegro"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить технические флаги заказа (сток, инвойс).
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может получать только свои заказы.**
    """
    try:
        # Импортируем сервис здесь для избежания циклических импортов
        from app.services.order_technical_flags_service import OrderTechnicalFlagsService
        
        # Валидируем принадлежность токена пользователю
        validate_token_and_get_service(token_id, current_user)
        
        # Получаем технические флаги (с автосозданием при необходимости)
        with OrderTechnicalFlagsService(current_user.user_id, token_id) as flags_service:
            flags = flags_service.get_or_create_flags(order_id)
            
            return {
                "order_id": order_id,
                "technical_flags": {
                    "is_stock_updated": flags.is_stock_updated,
                    "has_invoice_created": flags.has_invoice_created,
                    "invoice_id": flags.invoice_id,
                    "created_at": flags.created_at.isoformat(),
                    "updated_at": flags.updated_at.isoformat()
                }
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка получения технических флагов: {str(e)}"
        )


@router.get("/technical-flags/summary",
          response_model=Dict[str, Any],
          summary="Сводка по техническим флагам",
          description="Получение статистики по техническим флагам всех заказов токена")
async def get_technical_flags_summary(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить сводную статистику по техническим флагам всех заказов токена.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        # Импортируем сервис здесь для избежания циклических импортов
        from app.services.order_technical_flags_service import OrderTechnicalFlagsService
        
        # Валидируем принадлежность токена пользователю
        validate_token_and_get_service(token_id, current_user)
        
        # Получаем сводку флагов
        with OrderTechnicalFlagsService(current_user.user_id, token_id) as flags_service:
            summary = flags_service.get_flags_summary()
            
            return {
                "token_id": str(token_id),
                "summary": summary,
                "generated_at": datetime.utcnow().isoformat()
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка получения сводки флагов: {str(e)}"
        ) 