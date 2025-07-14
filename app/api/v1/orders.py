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

class OrderSummary(BaseModel):
    """Краткая информация о заказе"""
    id: int
    order_id: str
    status: str
    buyer_email: Optional[str] = None
    buyer_name: Optional[str] = None
    total_amount: Optional[float] = None
    currency: Optional[str] = None
    line_items_count: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    allegro_revision: Optional[str] = None

class OrderDetails(BaseModel):
    """Детальная информация о заказе"""
    id: int
    order_id: str
    status: str
    buyer_data: Optional[Dict[str, Any]] = None
    line_items: Optional[List[Dict[str, Any]]] = None
    delivery_data: Optional[Dict[str, Any]] = None
    payment_data: Optional[Dict[str, Any]] = None
    total_price_amount: Optional[float] = None
    total_price_currency: Optional[str] = None
    allegro_revision: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class OrdersList(BaseModel):
    """Список заказов с пагинацией"""
    orders: List[OrderSummary]
    pagination: Dict[str, Any]
    filters: Dict[str, Any]

class SearchResults(BaseModel):
    """Результаты поиска заказов"""
    query: str
    results: List[Dict[str, Any]]
    total_found: int

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
          response_model=OrdersList, 
          summary="Получить список заказов",
          description="Получение списка заказов с фильтрацией и пагинацией")
async def get_orders(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    limit: int = Query(50, ge=1, le=100, description="Количество заказов на странице"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    status: Optional[str] = Query(None, description="Фильтр по статусу заказа"),
    from_date: Optional[datetime] = Query(None, description="Заказы от указанной даты"),
    to_date: Optional[datetime] = Query(None, description="Заказы до указанной даты"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить список заказов текущего пользователя с фильтрацией и пагинацией.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.get_orders_list(
            limit=limit,
            offset=offset,
            status_filter=status,
            from_date=from_date,
            to_date=to_date
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения заказов: {str(e)}")


@router.get("/search",
          response_model=SearchResults,
          summary="Поиск заказов",
          description="Поиск заказов по email покупателя, ID заказа или имени")
async def search_orders(
    token_id: UUID = Query(..., description="ID токена для доступа к Allegro API"),
    query: str = Query(..., min_length=3, description="Поисковый запрос (минимум 3 символа)"),
    limit: int = Query(50, ge=1, le=100, description="Максимальное количество результатов"),
    current_user: CurrentUser = CurrentUserDep
):
    """
    Поиск заказов текущего пользователя по различным критериям.
    
    **Требует аутентификации через JWT токен.**
    """
    try:
        order_service = validate_token_and_get_service(token_id, current_user)
        
        result = order_service.search_orders(
            search_query=query,
            limit=limit
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